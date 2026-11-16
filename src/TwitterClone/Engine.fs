// Twitter clone, Part II: the engine. One Akka.NET actor owns all of the state (users, followers,
// feeds, the hashtag and mention indexes, sessions and open WebSocket connections) and handles one
// command at a time, so requests that arrive together cannot interfere with each other.
// Authors: Michael Perez and Blas Kojusner

module TwitterClone.Engine

open System
open System.Threading.Tasks
open Akka.Actor
open Akka.FSharp
open TwitterClone.Domain

/// What the engine sends to an open WebSocket connection.
type Push =
    /// The most recent tweets of the user's feed, sent once when the connection opens
    | FeedSnapshot of Tweet list
    /// A new tweet in the user's feed
    | NewTweet of Tweet
    /// Someone started following the user
    | NewFollower of username: string
    /// The connection is to be closed, with this WebSocket close code and reason
    | Closing of code: int * reason: string

type EngineError =
    /// The session token is missing, unknown or has been logged out
    | NotLoggedIn
    /// The request breaks one of the rules in Domain
    | Rejected of DomainError
    /// The engine failed unexpectedly; its state was left as it was
    | Failed of message: string

type Command =
    | Register of username: string * password: PasswordHash
    | GetPassword of username: string
    | StartSession of username: string * token: string
    | EndSession of token: string
    | Follow of token: string * username: string
    | Unfollow of token: string * username: string
    | PostTweet of token: string * text: string
    | SearchMentions of term: string
    | SearchHashtags of term: string
    /// Registers an open WebSocket connection and sends it the user's feed
    | Connect of token: string * connectionId: Guid * push: (Push -> unit)
    /// Forgets a connection; sent with Tell, so there is no reply
    | Disconnect of connectionId: Guid
    | CloseAllConnections of reason: string
    | CountConnections of username: string

type private Connection =
    { Username: string
      Token: string
      Push: Push -> unit }

type private EngineState =
    { Data: State
      /// The user each session token belongs to
      Sessions: Map<string, string>
      Connections: Map<Guid, Connection> }

/// WebSocket close codes: normal closure, and the server going away.
[<Literal>]
let private CloseNormal = 1000

[<Literal>]
let private CloseGoingAway = 1001

let private handle (mailbox: Actor<Command>) (state: EngineState ref) (command: Command) =
    let sender = mailbox.Sender()
    let reply (result: Result<'T, EngineError>) = sender <! result

    // Runs a command against the current state. An exception becomes an error reply instead of
    // escaping, so no request can make the actor fail. Commands only change the state after all of
    // the work that could fail, and push to connections last.
    let run (command: EngineState -> Result<'T, EngineError>) : Result<'T, EngineError> =
        try
            command state.Value
        with ex ->
            logException mailbox ex
            Error(Failed ex.Message)

    // Queues a message for a connection. Each connection has its own sender, so this is quick, and a
    // connection that has gone away cannot affect the others.
    let deliver (connection: Connection) (push: Push) =
        try
            connection.Push push
        with ex ->
            logError mailbox (sprintf "Could not queue a message for @%s: %s" connection.Username ex.Message)

    let sessionUser (current: EngineState) (token: string) =
        if isNull token then None else Map.tryFind token current.Sessions

    let connectionsOf (username: string) (current: EngineState) =
        current.Connections |> Map.filter (fun _ connection -> connection.Username = username)

    match command with
    | Register(username, password) ->
        run (fun current ->
            match register username password current.Data with
            | Ok data ->
                state.Value <- { current with Data = data }
                Ok()
            | Error error -> Error(Rejected error))
        |> reply

    | GetPassword username ->
        run (fun current ->
            match passwordOf username current.Data with
            | Some password -> Ok password
            | None -> Error(Rejected(UnknownUser username)))
        |> reply

    | StartSession(username, token) ->
        run (fun current ->
            if not (isRegistered username current.Data) then
                Error(Rejected(UnknownUser username))
            else
                state.Value <- { current with Sessions = Map.add token username current.Sessions }
                Ok())
        |> reply

    | EndSession token ->
        run (fun current ->
            match sessionUser current token with
            | None -> Error NotLoggedIn
            | Some username ->
                let closing, remaining = current.Connections |> Map.partition (fun _ connection -> connection.Token = token)

                state.Value <-
                    { current with
                        Sessions = Map.remove token current.Sessions
                        Connections = remaining }

                for KeyValue(_, connection) in closing do
                    deliver connection (Closing(CloseNormal, "Logged out."))

                Ok username)
        |> reply

    | Follow(token, target) ->
        run (fun current ->
            match sessionUser current token with
            | None -> Error NotLoggedIn
            | Some username ->
                match follow username target current.Data with
                | Error error -> Error(Rejected error)
                | Ok data ->
                    state.Value <- { current with Data = data }

                    for KeyValue(_, connection) in connectionsOf target current do
                        deliver connection (NewFollower username)

                    Ok())
        |> reply

    | Unfollow(token, target) ->
        run (fun current ->
            match sessionUser current token with
            | None -> Error NotLoggedIn
            | Some username ->
                match unfollow username target current.Data with
                | Error error -> Error(Rejected error)
                | Ok data ->
                    state.Value <- { current with Data = data }
                    Ok())
        |> reply

    | PostTweet(token, text) ->
        run (fun current ->
            match sessionUser current token with
            | None -> Error NotLoggedIn
            | Some username ->
                match postTweet username text DateTimeOffset.UtcNow current.Data with
                | Error error -> Error(Rejected error)
                | Ok(data, tweet) ->
                    state.Value <- { current with Data = data }
                    let audience = audienceOf username data

                    for KeyValue(_, connection) in current.Connections do
                        if audience.Contains connection.Username then
                            deliver connection (NewTweet tweet)

                    Ok tweet)
        |> reply

    | SearchMentions term -> run (fun current -> searchMentions term current.Data |> Result.mapError Rejected) |> reply

    | SearchHashtags term -> run (fun current -> searchHashtags term current.Data |> Result.mapError Rejected) |> reply

    | Connect(token, connectionId, push) ->
        run (fun current ->
            match sessionUser current token with
            | None -> Error NotLoggedIn
            | Some username ->
                let connection =
                    { Username = username
                      Token = token
                      Push = push }

                state.Value <- { current with Connections = Map.add connectionId connection current.Connections }
                deliver connection (FeedSnapshot(feedOf username current.Data))
                Ok username)
        |> reply

    | Disconnect connectionId ->
        run (fun current ->
            state.Value <- { current with Connections = Map.remove connectionId current.Connections }
            Ok())
        |> ignore

    | CloseAllConnections reason ->
        run (fun current ->
            state.Value <- { current with Connections = Map.empty }

            for KeyValue(_, connection) in current.Connections do
                deliver connection (Closing(CloseGoingAway, reason))

            Ok current.Connections.Count)
        |> reply

    | CountConnections username -> run (fun current -> Ok(connectionsOf username current |> Map.count)) |> reply

/// Typed access to the engine actor. Each call asks the actor and completes with its reply.
type Engine internal (actor: IActorRef, timeout: TimeSpan) =
    member private _.Ask<'T>(command: Command) : Task<Result<'T, EngineError>> =
        actor.Ask<Result<'T, EngineError>>(command, Nullable timeout)

    member this.Register(username: string, password: PasswordHash) : Task<Result<unit, EngineError>> =
        this.Ask(Register(username, password))

    /// The stored password hash of a user, so the caller can check a password outside the actor
    member this.GetPassword(username: string) : Task<Result<PasswordHash, EngineError>> =
        this.Ask(GetPassword username)

    member this.StartSession(username: string, token: string) : Task<Result<unit, EngineError>> =
        this.Ask(StartSession(username, token))

    /// Ends a session and closes the WebSocket connections opened with it; replies with the username
    member this.EndSession(token: string) : Task<Result<string, EngineError>> = this.Ask(EndSession token)

    member this.Follow(token: string, username: string) : Task<Result<unit, EngineError>> =
        this.Ask(Follow(token, username))

    member this.Unfollow(token: string, username: string) : Task<Result<unit, EngineError>> =
        this.Ask(Unfollow(token, username))

    member this.PostTweet(token: string, text: string) : Task<Result<Tweet, EngineError>> =
        this.Ask(PostTweet(token, text))

    member this.SearchMentions(term: string) : Task<Result<Tweet list, EngineError>> = this.Ask(SearchMentions term)

    member this.SearchHashtags(term: string) : Task<Result<Tweet list, EngineError>> = this.Ask(SearchHashtags term)

    /// Registers a WebSocket connection; replies with the username, and push receives the feed first
    member this.Connect(token: string, connectionId: Guid, push: Push -> unit) : Task<Result<string, EngineError>> =
        this.Ask(Connect(token, connectionId, push))

    member _.Disconnect(connectionId: Guid) = actor.Tell(Disconnect connectionId)

    /// Asks every open connection to close; replies with how many there were
    member this.CloseAllConnections(reason: string) : Task<Result<int, EngineError>> =
        this.Ask(CloseAllConnections reason)

    /// How many WebSocket connections a user has open
    member this.CountConnections(username: string) : Task<Result<int, EngineError>> =
        this.Ask(CountConnections username)

/// Starts the engine actor in an actor system.
let start (system: ActorSystem) (askTimeout: TimeSpan) =
    // The state is kept outside the actor's body: Akka.NET restarts a failed actor by running its
    // body again, which would otherwise start it over with no users. Commands catch their own
    // exceptions, so this is a second line of defence.
    let state =
        ref
            { Data = emptyState
              Sessions = Map.empty
              Connections = Map.empty }

    let actorRef =
        spawn system "engine" (fun (mailbox: Actor<Command>) ->
            let rec loop () =
                actor {
                    let! command = mailbox.Receive()
                    handle mailbox state command
                    return! loop ()
                }

            loop ())

    Engine(actorRef, askTimeout)
