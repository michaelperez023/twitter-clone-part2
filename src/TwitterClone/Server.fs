// Twitter clone, Part II: the web server. Suave serves the page, a JSON API and a WebSocket feed,
// and the engine actor handles every request.
// Authors: Michael Perez and Blas Kojusner
// sources used: https://github.com/SuaveIO/suave/tree/master/examples/WebSocket

module TwitterClone.Server

open System
open System.Buffers.Text
open System.Collections.Concurrent
open System.Globalization
open System.IO
open System.Net
open System.Net.Sockets
open System.Security.Cryptography
open System.Text
open System.Text.Json
open System.Threading
open System.Threading.Tasks
open Akka.Actor
open Akka.Configuration
open Suave
open Suave.Filters
open Suave.Operators
open Suave.WebSocket
open TwitterClone.Domain
open TwitterClone.Engine

type Settings =
    { Host: IPAddress
      Port: int
      /// PBKDF2 iterations for new passwords (the tests use a small number)
      PasswordIterations: int
      /// The folder with index.html, app.js and style.css
      WebRoot: string }

let defaultSettings =
    { Host = IPAddress.Loopback
      Port = 8080
      PasswordIterations = DefaultPasswordIterations
      WebRoot = Path.Combine(AppContext.BaseDirectory, "wwwroot") }

// ---------------------------------------------------------------------------------------------
// JSON: request bodies, replies and WebSocket messages
// ---------------------------------------------------------------------------------------------

type CredentialsBody = { Username: string; Password: string }

type UsernameBody = { Username: string }

type TweetBody = { Text: string }

type SocketHello = { Token: string }

type TweetJson =
    { Id: int64
      Author: string
      Text: string
      /// ISO 8601, UTC
      Time: string }

type Reply = { Ok: bool; Message: string }

type LoginReply =
    { Ok: bool
      Message: string
      Username: string
      Token: string }

type TweetReply =
    { Ok: bool
      Message: string
      Tweet: TweetJson }

type SearchReply =
    { Ok: bool
      Message: string
      Tweets: TweetJson[] }

type FeedMessage = { Type: string; Tweets: TweetJson[] }

type TweetMessage = { Type: string; Tweet: TweetJson }

type FollowMessage = { Type: string; From: string }

type ErrorMessage = { Type: string; Message: string }

let private jsonOptions =
    let options = JsonSerializerOptions(JsonSerializerDefaults.Web)
    // A body that leaves out a field is rejected instead of reading it as null
    options.RespectRequiredConstructorParameters <- true
    options

let private serialize (value: 'T) =
    JsonSerializer.SerializeToUtf8Bytes(value, jsonOptions)

/// Parses JSON into a record; None if it is not valid JSON of that shape.
let private tryParse<'T> (utf8: byte[]) =
    try
        match box (JsonSerializer.Deserialize<'T>(ReadOnlySpan<byte>(utf8), jsonOptions)) with
        | null -> None
        | value -> Some(unbox<'T> value)
    with :? JsonException ->
        None

let private toJson (tweet: Tweet) : TweetJson =
    { Id = tweet.Id
      Author = tweet.Author
      Text = tweet.Text
      Time = tweet.Time.UtcDateTime.ToString("yyyy-MM-dd'T'HH:mm:ss.fff'Z'", CultureInfo.InvariantCulture) }

let private toJsonArray (tweets: Tweet list) = tweets |> List.map toJson |> Array.ofList

// ---------------------------------------------------------------------------------------------
// HTTP API
// ---------------------------------------------------------------------------------------------

let private sendJson (status: HttpCode) (value: 'T) : WebPart =
    Writers.setMimeType "application/json; charset=utf-8"
    >=> Writers.setHeader "Cache-Control" "no-store"
    >=> Response.response status (serialize value)

let private reject (status: HttpCode) (message: string) =
    sendJson status { Reply.Ok = false; Message = message }

let private unauthorized (message: string) =
    Writers.setHeader "WWW-Authenticate" "Bearer" >=> reject HTTP_401 message

let private notLoggedIn =
    unauthorized "Log in first, and send the token from /api/login in an Authorization: Bearer header."

let private statusOf error =
    match error with
    | UnknownUser _ -> HTTP_404
    | UsernameTaken _
    | AlreadyFollowing _
    | NotFollowing _ -> HTTP_409
    | InvalidUsername
    | InvalidPassword
    | CannotFollowSelf
    | CannotUnfollowSelf
    | EmptyTweet
    | TweetTooLong _
    | UnregisteredMention _
    | EmptySearch -> HTTP_400

let private engineError error : WebPart =
    match error with
    | NotLoggedIn -> notLoggedIn
    | Rejected error -> reject (statusOf error) (describe error)
    | Failed _ -> reject HTTP_500 "The server could not handle the request."

/// Reads the request body as a JSON object; fields names the string fields it must have.
let private readBody<'T> (fields: string) (ctx: HttpContext) =
    match tryParse<'T> ctx.request.rawForm with
    | Some body -> Ok body
    | None -> Error(sprintf "The request body must be a JSON object with %s." fields)

let private credentialsFields = "the strings username and password"

let private bearerToken (ctx: HttpContext) =
    match ctx.request.header "authorization" with
    | Choice1Of2 value when value.StartsWith("Bearer ", StringComparison.OrdinalIgnoreCase) ->
        match value.Substring(7).Trim() with
        | "" -> None
        | token -> Some token
    | _ -> None

/// Runs the handler with the session token from the Authorization header, or answers 401.
let private loggedIn (handler: string -> WebPart) : WebPart =
    fun ctx ->
        match bearerToken ctx with
        | Some token -> handler token ctx
        | None -> notLoggedIn ctx

/// A random session token: 32 bytes, base64url-encoded.
let private newToken () =
    Base64Url.EncodeToString(ReadOnlySpan<byte>(RandomNumberGenerator.GetBytes 32))

let private register (engine: Engine) (iterations: int) : WebPart =
    fun ctx ->
        async {
            match readBody<CredentialsBody> credentialsFields ctx with
            | Error problem -> return! reject HTTP_400 problem ctx
            | Ok body ->
                match checkCredentials body.Username body.Password with
                | Error error -> return! reject HTTP_400 (describe error) ctx
                | Ok() ->
                    // Hashing is slow on purpose, so it happens here and not in the engine actor
                    let password = hashPassword iterations body.Password

                    match! Async.AwaitTask(engine.Register(body.Username, password)) with
                    | Ok() ->
                        let message = sprintf "@%s registered. You can log in now." body.Username
                        return! sendJson HTTP_201 { Reply.Ok = true; Message = message } ctx
                    | Error error -> return! engineError error ctx
        }

let private login (engine: Engine) (decoy: Lazy<PasswordHash>) : WebPart =
    fun ctx ->
        async {
            match readBody<CredentialsBody> credentialsFields ctx with
            | Error problem -> return! reject HTTP_400 problem ctx
            | Ok body ->
                match checkCredentials body.Username body.Password with
                | Error error -> return! reject HTTP_400 (describe error) ctx
                | Ok() ->
                    // The engine only hands out the stored hash; the password is checked here
                    match! Async.AwaitTask(engine.GetPassword body.Username) with
                    | Error(Rejected(UnknownUser _)) ->
                        // Hash anyway, so an unknown username takes as long as a wrong password
                        verifyPassword body.Password decoy.Value |> ignore
                        return! unauthorized "Wrong username or password." ctx
                    | Error error -> return! engineError error ctx
                    | Ok stored when not (verifyPassword body.Password stored) ->
                        return! unauthorized "Wrong username or password." ctx
                    | Ok _ ->
                        let token = newToken ()

                        match! Async.AwaitTask(engine.StartSession(body.Username, token)) with
                        | Ok() ->
                            let reply =
                                { LoginReply.Ok = true
                                  Message = sprintf "Logged in as @%s." body.Username
                                  Username = body.Username
                                  Token = token }

                            return! sendJson HTTP_200 reply ctx
                        | Error error -> return! engineError error ctx
        }

let private logout (engine: Engine) : WebPart =
    loggedIn (fun token ctx ->
        async {
            match! Async.AwaitTask(engine.EndSession token) with
            | Ok username -> return! sendJson HTTP_200 { Reply.Ok = true; Message = sprintf "@%s logged out." username } ctx
            | Error error -> return! engineError error ctx
        })

/// Follow or unfollow: the acting user comes from the session token, the other one from the body.
let private followOrUnfollow (change: string * string -> Task<Result<unit, EngineError>>) (pastTense: string) : WebPart =
    loggedIn (fun token ctx ->
        async {
            match readBody<UsernameBody> "the string username" ctx with
            | Error problem -> return! reject HTTP_400 problem ctx
            | Ok body ->
                match! Async.AwaitTask(change (token, body.Username)) with
                | Ok() -> return! sendJson HTTP_200 { Reply.Ok = true; Message = sprintf "You %s @%s." pastTense body.Username } ctx
                | Error error -> return! engineError error ctx
        })

let private tweet (engine: Engine) : WebPart =
    loggedIn (fun token ctx ->
        async {
            match readBody<TweetBody> "the string text" ctx with
            | Error problem -> return! reject HTTP_400 problem ctx
            | Ok body ->
                match! Async.AwaitTask(engine.PostTweet(token, body.Text)) with
                | Ok posted ->
                    let reply =
                        { TweetReply.Ok = true
                          Message = "Tweet posted."
                          Tweet = toJson posted }

                    return! sendJson HTTP_201 reply ctx
                | Error error -> return! engineError error ctx
        })

/// The summary of a search, such as "3 tweets mention @alice." There are at most ResultCount results.
let private summarize (count: int) (one: string) (many: string) =
    if count = 0 then sprintf "No tweets %s." many
    elif count = 1 then sprintf "1 tweet %s." one
    elif count < ResultCount then sprintf "%d tweets %s." count many
    else sprintf "The %d most recent tweets %s." count many

let private search (find: string -> Task<Result<Tweet list, EngineError>>) (summary: int -> string) (term: string) : WebPart =
    fun ctx ->
        async {
            match! Async.AwaitTask(find term) with
            | Ok tweets ->
                let reply =
                    { SearchReply.Ok = true
                      Message = summary tweets.Length
                      Tweets = toJsonArray tweets }

                return! sendJson HTTP_200 reply ctx
            | Error error -> return! engineError error ctx
        }

let private mentionSearch (engine: Engine) (term: string) =
    let username = searchTerm '@' term
    search engine.SearchMentions (fun count -> summarize count ("mentions @" + username) ("mention @" + username)) term

let private hashtagSearch (engine: Engine) (term: string) =
    let hashtag = "with #" + searchTerm '#' term
    search engine.SearchHashtags (fun count -> summarize count hashtag hashtag) term

/// Accepts only the given method; anything else gets 405.
let private only (methodFilter: WebPart) (methodName: string) (handler: WebPart) : WebPart =
    choose
        [ methodFilter >=> handler
          Writers.setHeader "Allow" methodName >=> reject HTTP_405 (sprintf "Use %s." methodName) ]

// ---------------------------------------------------------------------------------------------
// WebSocket feed
// ---------------------------------------------------------------------------------------------

/// WebSocket close codes used here.
[<Literal>]
let private CloseNormal = 1000

[<Literal>]
let private ClosePolicyViolation = 1008

[<Literal>]
let private CloseInternalError = 1011

/// How long a new connection has to send its token.
let private helloTimeout = TimeSpan.FromSeconds 10.0

/// The timing of an open feed. The page sends nothing after its token, but browsers answer pings
/// on their own, so a feed that stays silent too long has lost its connection. The pings also keep
/// a proxy in front of the server from closing a quiet feed.
type internal FeedTimings =
    { /// How long the sender waits between pings
      PingInterval: TimeSpan
      /// How long the client may send nothing at all (not even a pong). It is checked each time a ping
      /// is due, so a silent connection is ended at the first ping time after this
      SilenceLimit: TimeSpan
      /// How long the client has to answer the server's close frame with its own
      CloseGrace: TimeSpan }

let internal feedTimings =
    { PingInterval = TimeSpan.FromSeconds 30.0
      SilenceLimit = TimeSpan.FromSeconds 75.0
      CloseGrace = TimeSpan.FromSeconds 5.0 }

/// The payload of a close frame: the status code, then the reason (at most 123 bytes in all).
let private closePayload (code: int) (reason: string) =
    let reasonBytes = Encoding.UTF8.GetBytes reason |> Array.truncate 120
    Array.append [| byte (code >>> 8); byte code |] reasonBytes

let private errorMessage (message: string) =
    serialize { ErrorMessage.Type = "error"; Message = message }

type private Outgoing =
    | Deliver of Push
    | SendError of message: string
    | SendPing
    | SendPong of payload: byte[]
    | SendClose of code: int * reason: string * finished: AsyncReplyChannel<unit>

/// Sends the frames of one WebSocket connection, one at a time and in order. send writes one frame
/// and returns false if that failed. endConnection is called (once) when a send fails, when the
/// client stays silent past the SilenceLimit, or when it does not answer the server's close frame;
/// nothing here throws back into the engine.
type internal FeedSender(send: Opcode -> byte[] -> Async<bool>, endConnection: unit -> unit, timings: FeedTimings) =
    let stop = new CancellationTokenSource()
    let mutable closed = false
    let mutable ended = 0
    let mutable lastHeard = Environment.TickCount64

    let endOnce () =
        if Interlocked.Exchange(&ended, 1) = 0 then
            endConnection ()

    let sendData (opcode: Opcode) (payload: byte[]) =
        async {
            if not closed then
                let! sent = send opcode payload

                if not sent then
                    closed <- true
                    endOnce ()
        }

    let sendClose (code: int) (reason: string) =
        async {
            if not closed then
                closed <- true
                let! _ = send Opcode.Close (closePayload code reason)

                // The client answers with a close frame of its own, which ends the connection. If it
                // does not, end the connection anyway (Stop cancels this)
                Async.Start(
                    async {
                        do! Async.Sleep timings.CloseGrace
                        endOnce ()
                    },
                    stop.Token
                )
        }

    let outbox =
        MailboxProcessor<Outgoing>.Start(
            (fun inbox ->
                let rec loop () =
                    async {
                        match! inbox.Receive() with
                        | Deliver(FeedSnapshot tweets) ->
                            do! sendData Opcode.Text (serialize { FeedMessage.Type = "feed"; Tweets = toJsonArray tweets })
                        | Deliver(NewTweet posted) ->
                            do! sendData Opcode.Text (serialize { TweetMessage.Type = "tweet"; Tweet = toJson posted })
                        | Deliver(NewFollower username) ->
                            do! sendData Opcode.Text (serialize { FollowMessage.Type = "follow"; From = username })
                        | Deliver(Closing(code, reason)) -> do! sendClose code reason
                        | SendError message -> do! sendData Opcode.Text (errorMessage message)
                        | SendPing -> do! sendData Opcode.Ping [||]
                        | SendPong payload -> do! sendData Opcode.Pong payload
                        | SendClose(code, reason, finished) ->
                            do! sendClose code reason
                            finished.Reply()

                        return! loop ()
                    }

                loop ()),
            stop.Token
        )

    // Runs apart from the outbox, so that a send stuck on a dead connection cannot hold it up
    let rec heartbeat () =
        async {
            do! Async.Sleep timings.PingInterval

            if Environment.TickCount64 - lastHeard > int64 timings.SilenceLimit.TotalMilliseconds then
                endOnce ()
            else
                outbox.Post SendPing
                return! heartbeat ()
        }

    do Async.Start(heartbeat (), stop.Token)

    /// Queues a message from the engine
    member _.Push(push: Push) = outbox.Post(Deliver push)

    /// Notes that a frame (of any kind) arrived from the client
    member _.Heard() = lastHeard <- Environment.TickCount64

    member _.Pong(payload: byte[]) = outbox.Post(SendPong payload)

    /// Sends a close frame after everything queued before it, and waits until it has been sent
    member _.CloseAsync(code: int, reason: string) : Task =
        outbox.PostAndTryAsyncReply((fun finished -> SendClose(code, reason, finished)), 5000)
        |> Async.Ignore
        |> Async.StartAsTask
        :> Task

    /// Sends {"type": "error", ...} and closes the connection
    member this.FailAsync(code: int, message: string) : Task =
        outbox.Post(SendError message)
        this.CloseAsync(code, message)

    /// Stops the sending and the timers once the connection has ended. endConnection is not called
    /// after this, since the connection may by then be serving another client.
    member _.Stop() =
        Interlocked.Exchange(&ended, 1) |> ignore
        stop.Cancel()

/// Writes one frame; false if the connection failed.
let private sendFrame (webSocket: WebSocket) (opcode: Opcode) (payload: byte[]) =
    async {
        try
            let! result = webSocket.send opcode (Memory<byte>(payload)) true |> _.AsTask() |> Async.AwaitTask

            match result with
            | Ok() -> return true
            | Error _ -> return false
        with _ ->
            return false
    }

/// How a connection began.
type private Opening =
    | SentToken of token: string
    | SentSomethingElse of message: string
    | ClosedFirst
    | Dropped

/// Reads the first message of a connection, which must be {"token": "..."}.
let private readOpening (webSocket: WebSocket) : Task<Opening> =
    task {
        let wrongMessage =
            SentSomethingElse "The first message must be a JSON object with the token from /api/login."

        let read = webSocket.read().AsTask()
        let! first = Task.WhenAny(read :> Task, Task.Delay helloTimeout)

        if not (Object.ReferenceEquals(first, read)) then
            return SentSomethingElse(sprintf "No token arrived within %g seconds of connecting." helloTimeout.TotalSeconds)
        else
            match read.Result with
            | Ok(Opcode.Text, data, true) ->
                match tryParse<SocketHello> (data.ToArray()) with
                | Some hello when not (String.IsNullOrWhiteSpace hello.Token) -> return SentToken hello.Token
                | _ -> return wrongMessage
            | Ok(Opcode.Close, _, _) -> return ClosedFirst
            | Ok _ -> return wrongMessage
            | Error _ -> return Dropped
    }

/// Reads until the client closes the connection or it drops (or the sender ends it). The page sends
/// nothing after its token, so other messages are ignored; pings are answered.
let private readUntilClosed (webSocket: WebSocket) (sender: FeedSender) : Task =
    task {
        let mutable reading = true

        while reading do
            match! webSocket.read() with
            | Ok(opcode, payload, _) ->
                sender.Heard()

                match opcode with
                | Opcode.Close ->
                    reading <- false
                    do! sender.CloseAsync(CloseNormal, "")
                | Opcode.Ping -> sender.Pong(payload.ToArray())
                | _ -> ()
            | Error _ -> reading <- false
    }

/// One WebSocket connection: authenticate it with the session token, register it with the engine
/// (which sends the feed and then live updates through its sender) and read until it ends. However
/// it ends, the finally block unregisters exactly this connection. openFeeds holds a task for each
/// connection that completes when it has ended.
let private feedSocket (engine: Engine) (openFeeds: ConcurrentDictionary<Guid, Task>) (webSocket: WebSocket) (ctx: HttpContext) =
    let connectionId = Guid.NewGuid()
    let ended = TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously)
    openFeeds[connectionId] <- ended.Task

    let sender =
        FeedSender(
            sendFrame webSocket,
            (fun () ->
                // Stop sending to this connection and end it, which ends the read below
                engine.Disconnect connectionId

                try
                    ctx.connection.transport.shutdown ()
                with _ ->
                    ()),
            feedTimings
        )

    ValueTask<Result<unit, Suave.Sockets.Error>>(
        task {
            try
                try
                    match! readOpening webSocket with
                    | SentToken token ->
                        match! engine.Connect(token, connectionId, sender.Push) with
                        | Ok _ -> ()
                        | Error NotLoggedIn -> do! sender.FailAsync(ClosePolicyViolation, "Your session has ended. Log in again.")
                        | Error _ -> do! sender.FailAsync(CloseInternalError, "The server could not open your feed.")

                        // Read until the connection ends. After an error that is when the page answers
                        // the close frame, so the page sees the close code (it logs out on 1008)
                        do! readUntilClosed webSocket sender
                    | SentSomethingElse message -> do! sender.FailAsync(ClosePolicyViolation, message)
                    | ClosedFirst -> do! sender.CloseAsync(CloseNormal, "")
                    | Dropped -> ()
                with ex ->
                    eprintfn "WebSocket connection failed: %s" ex.Message
            finally
                engine.Disconnect connectionId
                sender.Stop()
                openFeeds.TryRemove connectionId |> ignore
                ended.SetResult()

            return Ok()
        }
    )

// ---------------------------------------------------------------------------------------------
// Routes and startup
// ---------------------------------------------------------------------------------------------

let private securityHeaders: WebPart =
    Writers.setHeader "X-Content-Type-Options" "nosniff"
    >=> Writers.setHeader "Referrer-Policy" "no-referrer"
    >=> Writers.setHeader
            "Content-Security-Policy"
            "default-src 'self'; connect-src 'self' ws: wss:; img-src 'self' data:; object-src 'none'; base-uri 'none'; form-action 'self'; frame-ancestors 'none'"

let private staticFile (filePath: string) : WebPart =
    choose [ GET; HEAD ]
    >=> Writers.setHeader "Cache-Control" "no-cache"
    >=> Files.file filePath

/// The web application: the page, the JSON API and the WebSocket feed.
let private app (engine: Engine) (openFeeds: ConcurrentDictionary<Guid, Task>) (settings: Settings) : WebPart =
    let decoy = lazy (hashPassword settings.PasswordIterations "decoy")
    let page name = staticFile (Path.Combine(settings.WebRoot, name))
    let post = only POST "POST"
    let get = only GET "GET"

    securityHeaders
    >=> choose
        [ path "/" >=> page "index.html"
          path "/app.js" >=> page "app.js"
          path "/style.css" >=> page "style.css"
          path "/ws" >=> handShake (feedSocket engine openFeeds)
          path "/api/register" >=> post (register engine settings.PasswordIterations)
          path "/api/login" >=> post (login engine decoy)
          path "/api/logout" >=> post (logout engine)
          path "/api/follow" >=> post (followOrUnfollow engine.Follow "followed")
          path "/api/unfollow" >=> post (followOrUnfollow engine.Unfollow "unfollowed")
          path "/api/tweet" >=> post (tweet engine)
          pathScan "/api/search/mentions/%s" (fun term -> get (mentionSearch engine term))
          pathScan "/api/search/hashtags/%s" (fun term -> get (hashtagSearch engine term))
          pathStarts "/api/" >=> reject HTTP_404 "There is no such API endpoint."
          RequestErrors.NOT_FOUND "Not found." ]

let private errorHandler (ex: Exception) (_: string) (ctx: HttpContext) =
    eprintfn "%O %s failed: %s" ctx.request.``method`` ctx.request.path ex.Message
    reject HTTP_500 "The server could not handle the request." ctx

let private akkaSettings =
    """
akka {
  loglevel = WARNING
  stdout-loglevel = WARNING
  log-dead-letters = off
  log-dead-letters-during-shutdown = off
}
"""

/// True for 0.0.0.0 and ::, which listen on all network interfaces.
let listensOnAllInterfaces (host: IPAddress) =
    host.Equals IPAddress.Any || host.Equals IPAddress.IPv6Any

/// The URL to open in a browser on this computer. Browsers cannot open 0.0.0.0 or ::, so a server
/// that listens on all interfaces gets the loopback address, which it listens on as well.
let internal urlOf (host: IPAddress) (port: int) =
    let host =
        if host.Equals IPAddress.Any then IPAddress.Loopback
        elif host.Equals IPAddress.IPv6Any then IPAddress.IPv6Loopback
        else host

    let hostText =
        if host.AddressFamily = AddressFamily.InterNetworkV6 then
            sprintf "[%O]" host
        else
            string host

    sprintf "http://%s:%d/" hostText port

/// A running server. Stop (or Dispose) closes the open feeds and stops the web server and the
/// actor system.
type RunningServer
    internal
    (
        system: ActorSystem,
        engine: Engine,
        openFeeds: ConcurrentDictionary<Guid, Task>,
        url: string,
        stopWebServer: CancellationTokenSource,
        webServer: Task
    ) =
    let mutable stopped = 0

    member _.Url = url
    member _.Engine = engine

    member _.Stop() =
        if Interlocked.Exchange(&stopped, 1) = 0 then
            // Close the feeds first (code 1001) and give the pages a moment to answer, so that they see a
            // clean close rather than a dropped connection. Stopping the web server cuts any left open.
            try
                engine.CloseAllConnections("The server is stopping.").Wait(TimeSpan.FromSeconds 5.0) |> ignore
                Task.WhenAll(openFeeds.Values).Wait(TimeSpan.FromSeconds 2.0) |> ignore
            with _ ->
                ()

            stopWebServer.Cancel()

            try
                webServer.Wait(TimeSpan.FromSeconds 10.0) |> ignore
            with _ ->
                ()

            system.Terminate().Wait(TimeSpan.FromSeconds 10.0) |> ignore
            stopWebServer.Dispose()

    interface IDisposable with
        member this.Dispose() = this.Stop()

/// Starts the engine and the web server, and returns once the server is listening.
/// Throws if the address cannot be used (for example, when the port is taken).
let start (settings: Settings) =
    let system = ActorSystem.Create("TwitterClone", ConfigurationFactory.ParseString akkaSettings)

    try
        let engine = TwitterClone.Engine.start system (TimeSpan.FromSeconds 10.0)
        let openFeeds = ConcurrentDictionary<Guid, Task>()
        let stopWebServer = new CancellationTokenSource()

        let config =
            { defaultConfig with
                bindings = [ HttpBinding.create HTTP settings.Host (uint16 settings.Port) ]
                cancellationToken = stopWebServer.Token
                hideStartupMessage = true
                errorHandler = errorHandler
                maxContentLength = 64 * 1024
                webSocketMaxFrameSize = 64 * 1024 }

        let listening, webServer = startWebServerAsync config (app engine openFeeds settings)

        try
            listening |> Async.RunSynchronously |> ignore
        with _ ->
            stopWebServer.Cancel()
            reraise ()

        new RunningServer(system, engine, openFeeds, urlOf settings.Host settings.Port, stopWebServer, webServer)
    with _ ->
        system.Terminate().Wait(TimeSpan.FromSeconds 10.0) |> ignore
        reraise ()
