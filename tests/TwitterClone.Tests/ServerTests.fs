module TwitterClone.Tests.ServerTests

open System
open System.Collections.Concurrent
open System.Globalization
open System.IO
open System.Net
open System.Net.Http
open System.Net.Http.Headers
open System.Net.Sockets
open System.Net.WebSockets
open System.Text
open System.Text.Json
open System.Threading
open System.Threading.Tasks
open Suave.WebSocket
open Xunit
open TwitterClone

/// How long to wait for a reply or a WebSocket message before failing.
let private timeout = TimeSpan.FromSeconds 10.0

/// Waits until the condition holds, checking it every 10 ms; fails after the timeout.
let private waitUntil (what: string) (condition: unit -> bool) =
    task {
        let deadline = DateTime.UtcNow + timeout

        while not (condition ()) && DateTime.UtcNow < deadline do
            do! Task.Delay 10

        Assert.True(condition (), sprintf "Timed out waiting until %s" what)
    }

[<Fact>]
let ``by default the server uses 600,000 iterations and finds the page next to the program`` () =
    Assert.Equal(600_000, Server.defaultSettings.PasswordIterations)
    // Not relative to the working directory, as it was in 2021
    Assert.Equal(Path.Combine(AppContext.BaseDirectory, "wwwroot"), Server.defaultSettings.WebRoot)
    Assert.True(File.Exists(Path.Combine(Server.defaultSettings.WebRoot, "index.html")))

[<Fact>]
let ``the printed URL opens in a browser, also when the server listens on all interfaces`` () =
    Assert.Equal("http://127.0.0.1:8080/", Server.urlOf IPAddress.Loopback 8080)
    Assert.Equal("http://192.168.1.20:9000/", Server.urlOf (IPAddress.Parse "192.168.1.20") 9000)
    Assert.Equal("http://[::1]:8080/", Server.urlOf IPAddress.IPv6Loopback 8080)
    // Browsers cannot open http://0.0.0.0:8080/, so the loopback address is printed instead
    Assert.Equal("http://127.0.0.1:8080/", Server.urlOf IPAddress.Any 8080)
    Assert.Equal("http://[::1]:8080/", Server.urlOf IPAddress.IPv6Any 8080)
    Assert.True(Server.listensOnAllInterfaces IPAddress.Any)
    Assert.True(Server.listensOnAllInterfaces(IPAddress.Parse "::"))
    Assert.False(Server.listensOnAllInterfaces IPAddress.Loopback)

// ---------------------------------------------------------------------------------------------
// The sender of a WebSocket connection, with a fake send function instead of a socket
// ---------------------------------------------------------------------------------------------

/// Timings that do not fire during a test unless the test changes them.
let private slowTimings: Server.FeedTimings =
    { PingInterval = TimeSpan.FromHours 1.0
      SilenceLimit = TimeSpan.FromHours 1.0
      CloseGrace = TimeSpan.FromHours 1.0 }

/// A frame as text: the JSON of a text frame, or the code and reason of a close frame.
let private frameText (opcode: Opcode) (payload: byte[]) =
    if opcode = Opcode.Close then
        sprintf "%d %s" ((int payload[0] <<< 8) ||| int payload[1]) (Encoding.UTF8.GetString(payload, 2, payload.Length - 2))
    else
        Encoding.UTF8.GetString payload

/// A FeedSender that records the frames it sends instead of sending them. fails says whether the
/// send of the n-th frame (counting from 1) fails; answer sees each frame, as a client would.
type private RecordingSender(timings: Server.FeedTimings, fails: int -> bool, answer: Opcode -> Server.FeedSender -> unit) as this =
    let frames = ConcurrentQueue<Opcode * string>()
    let endCount = [| 0 |]
    let ended = TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously)

    let send (opcode: Opcode) (payload: byte[]) =
        async {
            frames.Enqueue((opcode, frameText opcode payload))
            answer opcode this.Sender
            return not (fails frames.Count)
        }

    let endConnection () =
        Interlocked.Increment(&endCount[0]) |> ignore
        ended.TrySetResult() |> ignore

    let sender = Server.FeedSender(send, endConnection, timings)

    member _.Sender = sender
    /// The frames the sender tried to send, in order
    member _.Frames = List.ofSeq frames
    /// How many times the sender ended the connection
    member _.EndCount = Volatile.Read(&endCount[0])
    /// Completes when the sender ends the connection
    member _.Ended = ended.Task

    interface IDisposable with
        member _.Dispose() = sender.Stop()

let private noAnswer (_: Opcode) (_: Server.FeedSender) = ()

let private neverFails (_: int) = false

let private followed (username: string) =
    Opcode.Text, sprintf """{"type":"follow","from":"%s"}""" username

[<Fact>]
let ``a failed send ends that connection only, and nothing more is sent on it`` () : Task =
    task {
        // In 2021 one mailbox sent every user's messages, with no error handling
        use failing = new RecordingSender(slowTimings, (fun n -> n = 2), noAnswer)
        use healthy = new RecordingSender(slowTimings, neverFails, noAnswer)

        for name in [ "a"; "b"; "c" ] do
            failing.Sender.Push(Engine.NewFollower name)
            healthy.Sender.Push(Engine.NewFollower name)

        do! failing.Ended.WaitAsync timeout
        failing.Sender.Push(Engine.NewFollower "d")
        healthy.Sender.Push(Engine.NewFollower "d")

        // A close is sent after everything queued before it, so once it is done the rest has been handled
        do! failing.Sender.CloseAsync(1000, "")
        do! healthy.Sender.CloseAsync(1000, "")

        Assert.Equal<(Opcode * string) list>([ followed "a"; followed "b" ], failing.Frames)
        Assert.Equal(1, failing.EndCount)

        Assert.Equal<(Opcode * string) list>(
            [ followed "a"; followed "b"; followed "c"; followed "d"; (Opcode.Close, "1000 ") ],
            healthy.Frames
        )

        Assert.Equal(0, healthy.EndCount)
    }

[<Fact>]
let ``a connection is pinged, and ended when the client stops answering`` () : Task =
    task {
        let timings =
            { slowTimings with
                PingInterval = TimeSpan.FromMilliseconds 20.0
                SilenceLimit = TimeSpan.FromMilliseconds 200.0 }

        let pings (sender: RecordingSender) =
            sender.Frames |> List.filter (fun (opcode, _) -> opcode = Opcode.Ping) |> List.length

        // This client answers every ping, as browsers do
        let answerPings (opcode: Opcode) (sender: Server.FeedSender) =
            if opcode = Opcode.Ping then
                sender.Heard()

        use answering = new RecordingSender(timings, neverFails, answerPings)
        use silent = new RecordingSender(timings, neverFails, noAnswer)

        do! silent.Ended.WaitAsync timeout
        Assert.True(pings silent > 0)

        // Pinged for well over the silence limit, the answering client is still connected
        do! waitUntil "25 pings were sent" (fun () -> pings answering >= 25)
        Assert.Equal(0, answering.EndCount)
        Assert.Equal(1, silent.EndCount)
    }

[<Fact>]
let ``a client that does not answer the close frame is cut off, but only while the connection is open`` () : Task =
    task {
        let timings (grace: float) =
            { slowTimings with CloseGrace = TimeSpan.FromMilliseconds grace }

        use unanswered = new RecordingSender(timings 50.0, neverFails, noAnswer)
        do! unanswered.Sender.CloseAsync(1000, "Logged out.")
        do! unanswered.Ended.WaitAsync timeout
        Assert.Equal<(Opcode * string) list>([ Opcode.Close, "1000 Logged out." ], unanswered.Frames)

        // Here the client answers in time: the connection ends and the handler calls Stop
        use answered = new RecordingSender(timings 500.0, neverFails, noAnswer)
        do! answered.Sender.CloseAsync(1000, "Logged out.")
        answered.Sender.Stop()
        do! Task.Delay 1000
        Assert.Equal(0, answered.EndCount)
    }

let private freePort () =
    let listener = new TcpListener(IPAddress.Loopback, 0)
    listener.Start()
    let port = (listener.LocalEndpoint :?> IPEndPoint).Port
    listener.Stop()
    port

/// A real server (Suave and an actor system) on a free port, shared by the tests in this file.
type ServerFixture() =
    // Suave prints a line when it stops; keep the test output clean
    do Suave.Globals.messageWriter <- Some TextWriter.Null

    let server, port =
        // Another process could take the port before the server binds it, so try a few
        let rec start attempt =
            let port = freePort ()

            let settings =
                { Server.defaultSettings with
                    Port = port
                    PasswordIterations = 1_000 }

            try
                (Server.start settings, port)
            with _ when attempt < 5 ->
                start (attempt + 1)

        start 1

    let client = new HttpClient(BaseAddress = Uri(sprintf "http://127.0.0.1:%d/" port), Timeout = timeout)

    member _.Server = server
    member _.Port = port
    member _.Client = client

    interface IDisposable with
        member _.Dispose() =
            client.Dispose()
            server.Stop()

let mutable private lastId = 0

/// A username no other test uses (the tests share one server).
let private uniqueName (prefix: string) =
    sprintf "%s%d" prefix (Interlocked.Increment(&lastId))

let private json (value: 'T) = JsonSerializer.Serialize value

let private credentials (username: string) (password: string) =
    json {| username = username; password = password |}

type private Reply =
    { Status: HttpStatusCode
      Body: JsonElement
      Challenge: string }

    member this.Ok = this.Body.GetProperty("ok").GetBoolean()
    member this.Message = this.Body.GetProperty("message").GetString()

let private parseJson (text: string) =
    use document = JsonDocument.Parse(if String.IsNullOrEmpty text then "null" else text)
    document.RootElement.Clone()

let private tweetText (tweet: JsonElement) = tweet.GetProperty("text").GetString()

let private tweetTexts (tweets: JsonElement) =
    tweets.EnumerateArray() |> Seq.map tweetText |> List.ofSeq

/// The next WebSocket message, or Error with the close status if the server closed the connection.
let private receive (socket: ClientWebSocket) : Task<Result<JsonElement, WebSocketCloseStatus option>> =
    task {
        use cancel = new CancellationTokenSource(timeout)
        use message = new MemoryStream()
        let buffer = Array.zeroCreate<byte> 8192
        let mutable result = None

        while result.IsNone do
            let! received = socket.ReceiveAsync(ArraySegment<byte>(buffer), cancel.Token)

            if received.MessageType = WebSocketMessageType.Close then
                result <- Some(Error(Option.ofNullable socket.CloseStatus))
            else
                message.Write(buffer, 0, received.Count)

                if received.EndOfMessage then
                    result <- Some(Ok(parseJson (Encoding.UTF8.GetString(message.ToArray()))))

        return result.Value
    }

/// The next WebSocket message, which must not be a close.
let private next (socket: ClientWebSocket) =
    task {
        match! receive socket with
        | Ok message -> return message
        | Error status -> return failwithf "The server closed the feed (%A)" status
    }

/// A WebSocket connection to /ws, as the page opens it.
type private Feed(socket: ClientWebSocket, initial: JsonElement) =
    /// The tweets the server sent when the connection opened
    member _.Initial = initial
    member _.Socket = socket
    member _.Receive() = receive socket
    member _.Next() = next socket

    interface IDisposable with
        member _.Dispose() = socket.Dispose()

let private connectSocket (port: int) (firstMessage: string) =
    task {
        let socket = new ClientWebSocket()
        use cancel = new CancellationTokenSource(timeout)
        do! socket.ConnectAsync(Uri(sprintf "ws://127.0.0.1:%d/ws" port), cancel.Token)
        let bytes = Encoding.UTF8.GetBytes firstMessage
        do! socket.SendAsync(ArraySegment<byte>(bytes), WebSocketMessageType.Text, true, cancel.Token)
        return socket
    }

type ServerTests(fixture: ServerFixture) =
    let send (httpMethod: HttpMethod) (path: string) (token: string option) (body: string option) =
        task {
            use request = new HttpRequestMessage(httpMethod, path)

            match token with
            | Some token -> request.Headers.Authorization <- AuthenticationHeaderValue("Bearer", token)
            | None -> ()

            match body with
            | Some body -> request.Content <- new StringContent(body, Encoding.UTF8, "application/json")
            | None -> ()

            use! response = fixture.Client.SendAsync request
            let! text = response.Content.ReadAsStringAsync()

            return
                { Status = response.StatusCode
                  Body = parseJson text
                  Challenge = response.Headers.WwwAuthenticate |> Seq.map string |> String.concat ", " }
        }

    let post path token body = send HttpMethod.Post path token (Some body)
    let get path = send HttpMethod.Get path None None

    let register username =
        task {
            let! reply = post "api/register" None (credentials username ("pass-" + username))
            Assert.Equal(HttpStatusCode.Created, reply.Status)
        }

    let login username =
        task {
            let! reply = post "api/login" None (credentials username ("pass-" + username))
            Assert.Equal(HttpStatusCode.OK, reply.Status)
            return reply.Body.GetProperty("token").GetString()
        }

    let newUser prefix =
        task {
            let username = uniqueName prefix
            do! register username
            let! token = login username
            return username, token
        }

    let follow token username =
        task {
            let! reply = post "api/follow" (Some token) (json {| username = username |})
            Assert.Equal(HttpStatusCode.OK, reply.Status)
        }

    let tweet token text =
        task {
            let! reply = post "api/tweet" (Some token) (json {| text = text |})
            Assert.Equal(HttpStatusCode.Created, reply.Status)
            return reply.Body.GetProperty("tweet")
        }

    /// Opens a feed and reads the first message, which lists the most recent tweets.
    let openFeed token =
        task {
            let! socket = connectSocket fixture.Port (json {| token = token |})
            let! first = next socket
            Assert.Equal("feed", first.GetProperty("type").GetString())
            return new Feed(socket, first.GetProperty("tweets"))
        }

    /// Waits until the engine has this many open connections for the user.
    let waitForConnections username expected =
        task {
            let deadline = DateTime.UtcNow + timeout
            let mutable count = -1

            while count <> expected && DateTime.UtcNow < deadline do
                match! fixture.Server.Engine.CountConnections username with
                | Ok current -> count <- current
                | Error error -> failwithf "The engine failed: %A" error

                if count <> expected then
                    do! Task.Delay 20

            Assert.Equal(expected, count)
        }

    interface IClassFixture<ServerFixture>

    [<Fact>]
    member _.``GET / serves the page, and its script and styles are served too``() : Task =
        task {
            let! page = fixture.Client.GetAsync "/"
            Assert.Equal(HttpStatusCode.OK, page.StatusCode)
            Assert.Equal("text/html", page.Content.Headers.ContentType.MediaType)
            let! html = page.Content.ReadAsStringAsync()
            Assert.Contains("<title>Twitter Clone</title>", html)
            Assert.Contains("""<script src="app.js" defer></script>""", html)

            for path, mediaType in [ "/app.js", "application/javascript"; "/style.css", "text/css" ] do
                let! response = fixture.Client.GetAsync(path: string)
                Assert.Equal(HttpStatusCode.OK, response.StatusCode)
                Assert.Equal(mediaType, response.Content.Headers.ContentType.MediaType)

            let! missing = fixture.Client.GetAsync "/missing.txt"
            Assert.Equal(HttpStatusCode.NotFound, missing.StatusCode)
        }

    [<Fact>]
    member _.``register, log in and log out``() : Task =
        task {
            let username = uniqueName "reg"
            let! registered = post "api/register" None (credentials username "a long passphrase")
            Assert.Equal(HttpStatusCode.Created, registered.Status)
            Assert.True(registered.Ok)

            let! again = post "api/register" None (credentials username "another one")
            Assert.Equal(HttpStatusCode.Conflict, again.Status)
            Assert.False(again.Ok)

            let! invalid = post "api/register" None (credentials "no spaces allowed" "password")
            Assert.Equal(HttpStatusCode.BadRequest, invalid.Status)

            let! wrongPassword = post "api/login" None (credentials username "not the passphrase")
            Assert.Equal(HttpStatusCode.Unauthorized, wrongPassword.Status)
            let! unknownUser = post "api/login" None (credentials (uniqueName "nobody") "a long passphrase")
            Assert.Equal(HttpStatusCode.Unauthorized, unknownUser.Status)
            Assert.Equal(wrongPassword.Message, unknownUser.Message)

            let! loggedIn = post "api/login" None (credentials username "a long passphrase")
            Assert.Equal(HttpStatusCode.OK, loggedIn.Status)
            Assert.True(loggedIn.Ok)
            Assert.Equal(username, loggedIn.Body.GetProperty("username").GetString())
            let token = loggedIn.Body.GetProperty("token").GetString()
            // 32 random bytes in base64url, without padding
            Assert.Matches("^[A-Za-z0-9_-]{43}$", token)

            let! loggedOut = post "api/logout" (Some token) ""
            Assert.Equal(HttpStatusCode.OK, loggedOut.Status)
            let! secondLogout = post "api/logout" (Some token) ""
            Assert.Equal(HttpStatusCode.Unauthorized, secondLogout.Status)
        }

    [<Fact>]
    member _.``requests that need a session get 401 without a valid token``() : Task =
        task {
            let! _, token = newUser "auth"

            let requests =
                [ "api/tweet", json {| text = "hello" |}
                  "api/follow", json {| username = "someone" |}
                  "api/unfollow", json {| username = "someone" |}
                  "api/logout", "" ]

            for path, body in requests do
                let! noToken = post path None body
                Assert.Equal(HttpStatusCode.Unauthorized, noToken.Status)
                Assert.Equal("Bearer", noToken.Challenge)
                Assert.False(noToken.Ok)
                let! badToken = post path (Some "not-a-real-token") body
                Assert.Equal(HttpStatusCode.Unauthorized, badToken.Status)

            let! _ = tweet token "posted while logged in"
            let! _ = post "api/logout" (Some token) ""
            let! afterLogout = post "api/tweet" (Some token) (json {| text = "posted after logging out" |})
            Assert.Equal(HttpStatusCode.Unauthorized, afterLogout.Status)
        }

    [<Fact>]
    member _.``malformed JSON gets 400``() : Task =
        task {
            let! _, token = newUser "json"

            let requests =
                [ "api/register", None, "{not json"
                  "api/register", None, json {| username = "missing_password" |}
                  "api/login", None, "[]"
                  "api/login", None, ""
                  "api/tweet", Some token, json {| text = 5 |}
                  "api/follow", Some token, "null" ]

            for path, token, body in requests do
                let! reply = post path token body
                Assert.Equal(HttpStatusCode.BadRequest, reply.Status)
                Assert.False(reply.Ok)
                Assert.StartsWith("The request body must be a JSON object with", reply.Message)
        }

    [<Fact>]
    member _.``requests that arrive together all take effect``() : Task =
        task {
            // In 2021 the request handlers changed shared maps from many threads at once, so updates could be lost
            let username = uniqueName "busy"

            let! registrations =
                Task.WhenAll [| for _ in 1..20 -> post "api/register" None (credentials username ("pass-" + username)) |]

            let count status =
                registrations |> Array.filter (fun reply -> reply.Status = status) |> Array.length

            Assert.Equal(1, count HttpStatusCode.Created)
            Assert.Equal(19, count HttpStatusCode.Conflict)

            let! token = login username
            let! followers = Task.WhenAll [| for _ in 1..10 -> newUser "fan" |]

            let! follows =
                Task.WhenAll [| for _, followerToken in followers -> post "api/follow" (Some followerToken) (json {| username = username |}) |]

            Assert.All(follows, fun reply -> Assert.Equal(HttpStatusCode.OK, reply.Status))

            let! _ = tweet token "to every follower"

            for _, followerToken in followers do
                use! feed = openFeed followerToken
                Assert.Equal<string list>([ "to every follower" ], tweetTexts feed.Initial)
        }

    [<Fact>]
    member _.``a follow is pushed to the followed user, and follow rules give the right status codes``() : Task =
        task {
            let! alice, aliceToken = newUser "fa"
            let! bob, bobToken = newUser "fb"
            use! bobFeed = openFeed bobToken

            do! follow aliceToken bob
            let! notice = bobFeed.Next()
            Assert.Equal("follow", notice.GetProperty("type").GetString())
            Assert.Equal(alice, notice.GetProperty("from").GetString())

            let! twice = post "api/follow" (Some aliceToken) (json {| username = bob |})
            Assert.Equal(HttpStatusCode.Conflict, twice.Status)
            let! self = post "api/follow" (Some aliceToken) (json {| username = alice |})
            Assert.Equal(HttpStatusCode.BadRequest, self.Status)
            let! unknown = post "api/follow" (Some aliceToken) (json {| username = uniqueName "ghost" |})
            Assert.Equal(HttpStatusCode.NotFound, unknown.Status)
            let! notFollowing = post "api/unfollow" (Some bobToken) (json {| username = alice |})
            Assert.Equal(HttpStatusCode.Conflict, notFollowing.Status)
        }

    [<Fact>]
    member _.``a tweet is pushed to the author's followers and to the author``() : Task =
        task {
            let! _, aliceToken = newUser "ta"
            let! bob, bobToken = newUser "tb"
            let! _, carolToken = newUser "tc"
            do! follow aliceToken bob
            use! aliceFeed = openFeed aliceToken
            use! bobFeed = openFeed bobToken

            let! posted = tweet bobToken "Hello #fsharp"
            Assert.Equal(bob, posted.GetProperty("author").GetString())
            // ISO 8601 in UTC, with the Z
            let time = posted.GetProperty("time").GetString()
            Assert.Matches(@"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$", time)
            let parsed = DateTimeOffset.Parse(time, CultureInfo.InvariantCulture)
            Assert.InRange(parsed, DateTimeOffset.UtcNow.AddMinutes(-1.0), DateTimeOffset.UtcNow.AddMinutes(1.0))

            for feed in [ aliceFeed; bobFeed ] do
                let! pushed = feed.Next()
                Assert.Equal("tweet", pushed.GetProperty("type").GetString())
                let pushedTweet = pushed.GetProperty("tweet")
                Assert.Equal(posted.GetProperty("id").GetInt64(), pushedTweet.GetProperty("id").GetInt64())
                Assert.Equal("Hello #fsharp", tweetText pushedTweet)

            // carol does not follow bob, so it is not in her feed
            use! carolFeed = openFeed carolToken
            Assert.Equal(0, carolFeed.Initial.GetArrayLength())
        }

    [<Fact>]
    member _.``after an unfollow, tweets are no longer pushed``() : Task =
        task {
            let! alice, aliceToken = newUser "ua"
            let! bob, bobToken = newUser "ub"
            do! follow aliceToken bob
            use! aliceFeed = openFeed aliceToken

            let! _ = tweet bobToken "before the unfollow"
            let! before = aliceFeed.Next()
            Assert.Equal("before the unfollow", tweetText (before.GetProperty "tweet"))

            let! unfollowed = post "api/unfollow" (Some aliceToken) (json {| username = bob |})
            Assert.Equal(HttpStatusCode.OK, unfollowed.Status)
            let! _ = tweet bobToken "after the unfollow"

            // alice's own tweet reaches her feed; had bob's tweet been pushed, it would have come first
            let! _ = tweet aliceToken (sprintf "a tweet by @%s" alice)
            let! next = aliceFeed.Next()
            Assert.Equal(sprintf "a tweet by @%s" alice, tweetText (next.GetProperty "tweet"))

            use! again = openFeed aliceToken
            Assert.Equal<string list>([ sprintf "a tweet by @%s" alice; "before the unfollow" ], tweetTexts again.Initial)
        }

    [<Fact>]
    member _.``following a user who is offline keeps the engine running and its state intact``() : Task =
        task {
            // In 2021 this crashed the feed actor, which lost every feed and open connection
            let! alice, aliceToken = newUser "oa"
            let! bob, bobToken = newUser "ob"
            let! carol, carolToken = newUser "oc"
            do! follow aliceToken carol
            let! _ = tweet carolToken "earlier"
            use! aliceFeed = openFeed aliceToken
            Assert.Equal<string list>([ "earlier" ], tweetTexts aliceFeed.Initial)

            // bob has no open feed
            do! waitForConnections bob 0
            do! follow aliceToken bob

            // alice's connection and her earlier follow still work
            let! _ = tweet bobToken "from bob"
            let! fromBob = aliceFeed.Next()
            Assert.Equal("from bob", tweetText (fromBob.GetProperty "tweet"))
            let! _ = tweet carolToken "from carol"
            let! fromCarol = aliceFeed.Next()
            Assert.Equal("from carol", tweetText (fromCarol.GetProperty "tweet"))

            use! second = openFeed aliceToken
            Assert.Equal<string list>([ "from carol"; "from bob"; "earlier" ], tweetTexts second.Initial)
            do! waitForConnections alice 2
        }

    [<Fact>]
    member _.``a closed feed stops receiving, and the tweet is in the feed on reconnect``() : Task =
        task {
            let! alice, aliceToken = newUser "ca"
            let! bob, bobToken = newUser "cb"
            do! follow aliceToken bob

            // A normal close
            let! first = openFeed aliceToken
            do! waitForConnections alice 1
            use cancel = new CancellationTokenSource(timeout)
            do! first.Socket.CloseAsync(WebSocketCloseStatus.NormalClosure, "bye", cancel.Token)
            (first :> IDisposable).Dispose()
            do! waitForConnections alice 0

            let! _ = tweet bobToken "while you were away"
            use! second = openFeed aliceToken
            Assert.Equal<string list>([ "while you were away" ], tweetTexts second.Initial)

            // A connection that drops without a close frame is forgotten as well
            let! dropped = openFeed aliceToken
            do! waitForConnections alice 2
            dropped.Socket.Abort()
            (dropped :> IDisposable).Dispose()
            do! waitForConnections alice 1

            let! _ = tweet bobToken "still live"
            let! live = second.Next()
            Assert.Equal("still live", tweetText (live.GetProperty "tweet"))
        }

    [<Fact>]
    member _.``tweets with & $ < > and quotes come back unchanged``() : Task =
        task {
            let! _, aliceToken = newUser "sa"
            let! bob, bobToken = newUser "sb"
            do! follow aliceToken bob
            use! aliceFeed = openFeed aliceToken
            let tag = uniqueName "chars"

            let text =
                sprintf "Fish & chips for $5 <b>\"bold\"</b> 'single' \\ back <img src=x onerror=alert(1)> café \U0001F600 #%s" tag

            let! posted = tweet bobToken text
            Assert.Equal(text, tweetText posted)

            let! pushed = aliceFeed.Next()
            Assert.Equal(text, tweetText (pushed.GetProperty "tweet"))

            let! found = get ("api/search/hashtags/" + Uri.EscapeDataString("#" + tag))
            Assert.Equal(HttpStatusCode.OK, found.Status)
            Assert.Equal<string list>([ text ], tweetTexts (found.Body.GetProperty "tweets"))
        }

    [<Fact>]
    member _.``searches return the 10 most recent matches, newest first``() : Task =
        task {
            let! _, aliceToken = newUser "qa"
            let! bob, _ = newUser "qb"
            let tag = uniqueName "Topic"

            for n in 1..12 do
                let! _ = tweet aliceToken (sprintf "post %d #%s @%s" n tag bob)
                ()

            let expected = [ 12..-1..3 ] |> List.map (fun n -> sprintf "post %d #%s @%s" n tag bob)

            let! mentions = get ("api/search/mentions/" + Uri.EscapeDataString("@" + bob))
            Assert.Equal(HttpStatusCode.OK, mentions.Status)
            Assert.Equal<string list>(expected, tweetTexts (mentions.Body.GetProperty "tweets"))
            Assert.Equal(sprintf "The 10 most recent tweets mention @%s." bob, mentions.Message)

            // Hashtags match regardless of case, with or without the '#'
            for term in [ "#" + tag.ToLowerInvariant(); tag.ToUpperInvariant() ] do
                let! hashtags = get ("api/search/hashtags/" + Uri.EscapeDataString term)
                Assert.Equal<string list>(expected, tweetTexts (hashtags.Body.GetProperty "tweets"))

            let! none = get "api/search/hashtags/nothing_tagged_this"
            Assert.Equal(HttpStatusCode.OK, none.Status)
            Assert.Equal(0, (none.Body.GetProperty "tweets").GetArrayLength())

            let! empty = get "api/search/mentions/%40"
            Assert.Equal(HttpStatusCode.BadRequest, empty.Status)

            let! wrongMethod = post "api/search/mentions/someone" None ""
            Assert.Equal(HttpStatusCode.MethodNotAllowed, wrongMethod.Status)
        }

    [<Fact>]
    member _.``tweets are checked: empty, too long, or mentioning an unregistered user``() : Task =
        task {
            let! _, token = newUser "va"
            let ghost = uniqueName "ghost"

            for text, message in
                [ "   ", "Type something to tweet."
                  String('x', 281), "Tweets are at most 280 characters long; this one has 281."
                  sprintf "hi @%s" ghost, sprintf "@%s is not registered." ghost ] do
                let! reply = post "api/tweet" (Some token) (json {| text = text |})
                Assert.Equal(HttpStatusCode.BadRequest, reply.Status)
                Assert.Equal(message, reply.Message)

            let! longest = tweet token (String('x', 280))
            Assert.Equal(280, (tweetText longest).Length)
        }

    [<Fact>]
    member _.``a feed with a bad token gets an error message and is closed``() : Task =
        task {
            use! socket = connectSocket fixture.Port (json {| token = "not-a-real-token" |})
            let! message = next socket
            Assert.Equal("error", message.GetProperty("type").GetString())
            Assert.Equal("Your session has ended. Log in again.", message.GetProperty("message").GetString())
            let! closed = receive socket
            Assert.Equal(Error(Some WebSocketCloseStatus.PolicyViolation), closed |> Result.map ignore)
        }

    [<Fact>]
    member _.``logging out closes the feeds opened with that session``() : Task =
        task {
            let! alice, aliceToken = newUser "la"
            use! aliceFeed = openFeed aliceToken
            let! _ = post "api/logout" (Some aliceToken) ""
            let! closed = aliceFeed.Receive()
            Assert.Equal(Error(Some WebSocketCloseStatus.NormalClosure), closed |> Result.map ignore)
            do! waitForConnections alice 0
        }
