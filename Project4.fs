// Authors: Michael Perez and Blas Kojusner
// sources used: https://github.com/SuaveIO/suave/tree/master/examples/WebSocket
// RSA source used: http://www.fssnip.net/7WE/title/RSA-encryptiondecryption
open Akka.FSharp
open Akka.Actor
open Suave
open Suave.Filters
open Suave.Operators
open Suave.Successful
open Suave.Sockets
open Suave.Sockets.Control
open Suave.WebSocket
open Newtonsoft.Json
open FSharp.Json
open System.Security.Cryptography
open System.Text

let system = ActorSystem.Create("TwitterServer")

type Response = {
    username: string
    message: string
    action: string
    ack: string
}

type Request = {
    username: string
    value: string
}

type FeedActorMessages =
    | RegisterUserWithFeedActor of string
    | FollowUser of string * string
    | AddOnlineUser of string * WebSocket
    | RemoveOnlineUser of string
    | UpdateUserFeeds of string * string

// RSA encryption function
let RsaEncrypt (data : byte[]) (key : RSAParameters) (padding : bool) : byte[] =
    try
        let rsa = new RSACryptoServiceProvider()
        rsa.ImportParameters(key)
        rsa.Encrypt(data, padding)
    with
    | :? CryptographicException as e ->
        printfn "%A" e.Message
        Array.empty

// RSA decryption function
let RsaDecrypt (data : byte[]) (key : RSAParameters) (padding : bool) : byte[] =
    try
        let rsa = new RSACryptoServiceProvider()
        rsa.ImportParameters(key)
        rsa.Decrypt(data, padding)
    with
    | :? CryptographicException as e ->
        printfn "%A" e.Message
        Array.empty  

let socketSender = MailboxProcessor<string * WebSocket>.Start(fun inbox ->
    let rec messageLoop() = async {
        let! msg, webSocket = inbox.Receive()
        let byteRes = msg |> System.Text.Encoding.ASCII.GetBytes|> ByteSegment
        let! _ = webSocket.send Text byteRes true
        return! messageLoop()
    }
    messageLoop()
)

let FeedActor (mailbox:Actor<_>) = 
    let mutable usernameFollowersSetMap = Map.empty
    let mutable onlineUsernameWebSocketMap = Map.empty
    let mutable usernameFeedListMap = Map.empty
    let rec loop () = actor {
        let! message = mailbox.Receive()
        match message with
        | RegisterUserWithFeedActor(username) ->
            usernameFollowersSetMap <- Map.add username Set.empty usernameFollowersSetMap
            usernameFeedListMap <- Map.add username List.empty usernameFeedListMap
        | FollowUser(username, followedUsername) ->
            if usernameFollowersSetMap.ContainsKey followedUsername then
                usernameFollowersSetMap <- Map.add followedUsername (Set.add username usernameFollowersSetMap.[followedUsername]) usernameFollowersSetMap
                let jsonResponse = Json.serialize ({username = followedUsername; message = "@" + username + " followed you."; action = "Follow"; ack = "OK"})
                socketSender.Post (jsonResponse, onlineUsernameWebSocketMap.[followedUsername])
        | AddOnlineUser(username, userWebSocket) ->
            onlineUsernameWebSocketMap <- Map.add username userWebSocket onlineUsernameWebSocketMap
            let mutable msg = ""
            let mutable act = ""
            let mutable feed = ""
            let mutable feedLimit = 10
            if usernameFeedListMap.[username].Length = 0 then // feed for user logging in is empty
                act <- "Follow"
                msg <- "Nothing in feed yet."
            else  // feed has tweets
                if usernameFeedListMap.[username].Length < 10 then // limit feed to 10 tweets
                    feedLimit <- usernameFeedListMap.[username].Length
                for i in [0..(feedLimit-1)] do
                    feed <- "&" + usernameFeedListMap.[username].[i] + feed
                msg <- feed
                act <- "Feed"
            let jsonResponse = Json.serialize ({username = username; message = msg; action = act; ack = "OK"})
            socketSender.Post (jsonResponse, userWebSocket)
        | UpdateUserFeeds(username, msg) ->
            if usernameFollowersSetMap.ContainsKey username then
                for user in usernameFollowersSetMap.[username] do // for each of the user's followers
                    if usernameFollowersSetMap.ContainsKey user then // if the follower is registered
                        if onlineUsernameWebSocketMap.ContainsKey user then // if the follower is online
                            let jsonResponse = Json.serialize ({username = user; message = "@" + username + " tweeted:$" + msg; action = "Tweet"; ack = "OK"})
                            socketSender.Post (jsonResponse, onlineUsernameWebSocketMap.[user])
                        let mutable temp = []
                        if usernameFeedListMap.ContainsKey user then
                            temp <- usernameFeedListMap.[user]
                        usernameFeedListMap <- Map.add user (("@" + username + " tweeted:$" + msg) :: temp) usernameFeedListMap
        | RemoveOnlineUser(username) ->
            onlineUsernameWebSocketMap <- Map.remove username onlineUsernameWebSocketMap
        return! loop()
    }
    loop()

let feedActor = spawn system "FeedActor" FeedActor

let feed (webSocket : WebSocket) (context: HttpContext) =
    let rec loop() =
        let mutable user = ""
        socket {
            let! msg = webSocket.read() // the server will wait for a message to be received without blocking the thread
            // the message has type (Opcode * byte [] * bool)
            // Opcode type:
            //   type Opcode = Continuation | Text | Binary | Reserved | Close | Ping | Pong
            // byte [] contains the actual message. the last element is the FIN byte
            match msg with
            | (Text, data, true) ->
                user <- (Json.deserialize<Request> (UTF8.toString data)).username
                feedActor <! AddOnlineUser(user, webSocket)
                return! loop()
            | (Close, _, _) ->
                feedActor <! RemoveOnlineUser(user)
                do! webSocket.send Close ([||] |> ByteSegment) true // send close message to close web socket
            | _ -> return! loop()
        }
    loop()

// global maps for usernames->passswords, users->followers, hashtags->tweets, usernames->mentions
let mutable usernameEncryptedPasswordMap = Map.empty
let mutable usernameFollowersSetMap = Map.empty
let mutable hashtagTweetsListMap = Map.empty
let mutable usernameMentionsListMap = Map.empty

let rsa = new RSACryptoServiceProvider(2048) // Create new object with RSA library
let publicPrivateKeyXML = rsa.ToXmlString(true) // Obtain object with randomly generated public and private keys
rsa.FromXmlString(publicPrivateKeyXML) // Format the public and private key parts of the object in XML
let publicKey = rsa.ExportParameters(false) // Export rsa object with just public key
let privateKey = rsa.ExportParameters(true) // Export rsa object with public and private key

let registerUser input =
    let mutable response = ""
    if not (usernameEncryptedPasswordMap.ContainsKey input.username) then // the username is not registered already, valid request
        let plainTextArray = System.Text.Encoding.ASCII.GetBytes(input.value) // Convert input password into bytes
        let cipherArray = RsaEncrypt plainTextArray (publicKey) false // Send input password into the RsaEncrypt function

        usernameEncryptedPasswordMap <- Map.add input.username cipherArray usernameEncryptedPasswordMap
        usernameFollowersSetMap <- Map.add input.username Set.empty usernameFollowersSetMap
        feedActor <! RegisterUserWithFeedActor(input.username)
        response <- {username = input.username; message = "@" + input.username + " registered."; action = "Register"; ack = "OK"} |> Json.toJson |> System.Text.Encoding.UTF8.GetString
    else // invalid request, a user with the same username is already registered
        response <- {username = input.username; message = "Invalid request, @" + input.username + " is already registered."; action = "Register"; ack = "FAIL"} |> Json.toJson |> System.Text.Encoding.UTF8.GetString
    response

let loginUser input =
    let mutable response = ""
    if usernameEncryptedPasswordMap.ContainsKey input.username then // username is registered, valid request
        let decryptedTextArray = RsaDecrypt (usernameEncryptedPasswordMap.[input.username]) (privateKey) false // Send the password found on the map to be decrypted
        let decryptedText = System.Text.Encoding.ASCII.GetString(decryptedTextArray) // Convert the now decrypted password from the database into a string

        if input.value = decryptedText then
            response <- {username = input.username; message = "@" + input.username + " logged in."; action = "Login"; ack = "OK"} |> Json.toJson |> System.Text.Encoding.UTF8.GetString
        else
            response <- {username = input.username; message = "Invalid request, wrong username and/or password."; action = "Login"; ack = "FAIL"} |> Json.toJson |> System.Text.Encoding.UTF8.GetString
    else // username is not registered, invalid request
        response <- {username = input.username; message = "Invalid request, wrong username and/or password."; action = "Login"; ack = "FAIL"} |> Json.toJson |> System.Text.Encoding.UTF8.GetString
    response

let followUser input =
    let mutable response = ""
    if input.value <> input.username then // valid user to follow
        if usernameFollowersSetMap.ContainsKey input.value then // user that is being followed is registered
            if not (usernameFollowersSetMap.[input.value].Contains input.username) then // valid user to follow
                usernameFollowersSetMap <- Map.add input.value (Set.add input.username usernameFollowersSetMap.[input.value]) usernameFollowersSetMap
                feedActor <! FollowUser(input.username, input.value)
                response <- {username = input.username; message = "You followed @" + input.value + "."; action = "Follow"; ack = "OK"} |> Json.toJson |> System.Text.Encoding.UTF8.GetString
            else // user is already following the user they are requesting to follow, invalid request
                response <- {username = input.username; message = "Invalid request, you are already following @" + input.value + "."; action = "Follow"; ack = "FAIL"} |> Json.toJson |> System.Text.Encoding.UTF8.GetString
        else // user is trying to follow a user that is not registered, invalid request
            response <- {username = input.username; message = "Invalid request, @" + input.value + " is not registered."; action = "Follow"; ack = "FAIL"} |> Json.toJson |> System.Text.Encoding.UTF8.GetString
    else // user is trying to follow themself, invalid request
        response <- {username = input.username; message = "Invalid request, you cannot follow yourself."; action = "Follow"; ack = "FAIL"} |> Json.toJson |> System.Text.Encoding.UTF8.GetString
    response

let unfollowUser input =
    let mutable response = ""
    if input.value <> input.username then // valid user to unfollow
        if usernameFollowersSetMap.ContainsKey input.value then // user that is being unfollowed is registered
            if usernameFollowersSetMap.[input.value].Contains input.username then // if user follows the user being unfollowed
                let mutable temp = usernameFollowersSetMap.[input.value]
                temp <- Set.remove input.username usernameFollowersSetMap.[input.value]
                usernameFollowersSetMap <- Map.add input.value temp usernameFollowersSetMap
                response <- {username = input.username; message = "You unfollowed @" + input.value + "."; action = "Follow"; ack = "OK"} |> Json.toJson |> System.Text.Encoding.UTF8.GetString
            else // the user doesn't follow the user being unfollowed
                response <- {username = input.username; message = "Invalid request, you are not following @" + input.value + "."; action = "Follow"; ack = "FAIL"} |> Json.toJson |> System.Text.Encoding.UTF8.GetString
        else // user is trying to unfollow a user that is not registered, invalid request
            response <- {username = input.username; message = "Invalid request, @" + input.value + " is not registered."; action = "Follow"; ack = "FAIL"} |> Json.toJson |> System.Text.Encoding.UTF8.GetString
    else // user is trying to unfollow themself, invalid request
        response <- {username = input.username; message = "Invalid request, you cannot unfollow yourself."; action = "Follow"; ack = "FAIL"} |> Json.toJson |> System.Text.Encoding.UTF8.GetString 
    response

let tweetUser input =
    let mutable response = ""
    let mutable mention = ""
    let mutable hashtag = ""
    for word in (input.value.Split ' ') do // parse tweet and save mention and/or hashtag, if any
        if word.Length > 0 then
            if word.[0] = '@' then
                mention <- word.[1..(word.Length-1)]
            else if word.[0] = '#' then
                hashtag <- word.[1..(word.Length-1)]

    if hashtag <> "" then // add hashtag to hashtagTweetsListMap
        if not (hashtagTweetsListMap.ContainsKey hashtag) then
            hashtagTweetsListMap <- Map.add hashtag List.empty hashtagTweetsListMap
        hashtagTweetsListMap <- Map.add hashtag (("@" + input.username + " tweeted:$" + input.value) :: hashtagTweetsListMap.[hashtag]) hashtagTweetsListMap

    if mention <> "" then // add mention to usernameEncryptedPasswordMap, then tweet
        if usernameEncryptedPasswordMap.ContainsKey mention then // tweet with valid mention
            if not (usernameMentionsListMap.ContainsKey mention) then
                usernameMentionsListMap <- Map.add mention List.empty usernameMentionsListMap
            usernameMentionsListMap <- Map.add mention (("@" + input.username + " tweeted:$" + input.value) :: usernameMentionsListMap.[mention]) usernameMentionsListMap
            response <- {username = input.username; message = "@" + input.username + " tweeted:$" + input.value; action = "Tweet"; ack = "OK"} |> Json.toJson |> System.Text.Encoding.UTF8.GetString
            feedActor <! UpdateUserFeeds(input.username, input.value)
        else // mentioned user in tweet is not registered
            response <- {username = input.username; message = "Invalid request, @" + mention + " is not registered."; action = "Tweet"; ack = "FAIL"} |> Json.toJson |> System.Text.Encoding.UTF8.GetString
    else // tweet without mention
        feedActor <! UpdateUserFeeds(input.username, input.value)
        response <- {username = input.username; message = "@" + input.username + " tweeted:$" + input.value; action = "Tweet"; ack = "OK"} |> Json.toJson |> System.Text.Encoding.UTF8.GetString
    response

let searchMention (input:string) =
    let mutable response = ""
    let mutable results = ""
    let mutable resultsLimit = 10
    let mutable mention = ""
    if input.Length > 0 then // search is not white space, valid request
        if input.[0] = '@' then // search starts 
            mention <- input.[1..(input.Length-1)]
        else
            mention <- input
        if usernameMentionsListMap.ContainsKey mention then
            if (usernameMentionsListMap.[mention].Length < 10) then
                resultsLimit <- usernameMentionsListMap.[mention].Length
            for i in [0..(resultsLimit-1)] do
                results <- results + "&" + usernameMentionsListMap.[mention].[i]
            response <- Json.serialize {username = ""; message = results; action = "Search"; ack = "OK"}
        else
            response <- Json.serialize {username = ""; message = "Invalid request, no mentions found for @" + mention + "."; action = "Search"; ack = "FAIL"}
    else // search is white space, invalid request
        response <- Json.serialize {username = ""; message = "Invalid request, type something to search."; action = "Search"; ack = "FAIL"}
    response

let searchHashtag (input:string) =
    let mutable response = ""
    let mutable results = ""
    let mutable resultsLimit = 10
    if input.Length > 0 then // search is not white space, valid request
        if hashtagTweetsListMap.ContainsKey input then // hashtag has tweets, valid search
            if (hashtagTweetsListMap.[input].Length < 10) then // limit search results to 10
                resultsLimit <- hashtagTweetsListMap.[input].Length
            for i in [0..(resultsLimit-1)] do
                results <- results + "&" + hashtagTweetsListMap.[input].[i]
            response <- Json.serialize {username = ""; message = results; action = "Search"; ack = "OK"}
        else // hashtag does not have tweets, invalid search
            response <- Json.serialize {username = ""; message = "Invalid request, no tweets found for #" + input + "."; action = "Search"; ack = "FAIL"}
    else // search is white space, invalid request
        response <- Json.serialize {username = ""; message = "Invalid request, type a hashtag to search. (without '#')"; action = "Search"; ack = "FAIL"}
    response

// redirect request to different methods, deserialize and serialize messages
let redirect page method =
    let serialize input =
        JsonConvert.SerializeObject((method (JsonConvert.DeserializeObject(System.Text.Encoding.UTF8.GetString(input.rawForm), typeof<Request>) :?> Request)), JsonSerializerSettings())
        |> OK
    path ("/" + page) >=> POST >=> request serialize

// the different urls are handled and mapped to methods here
let app =
    choose [
        GET >=> path "/" >=> Files.file "index.html"
        redirect "login" loginUser
        redirect "register" registerUser
        redirect "follow" followUser
        redirect "unfollow" unfollowUser
        redirect "tweet" tweetUser
        path "/feed" >=> handShake feed
        pathScan "/searchMention/%s" (fun search -> OK (searchMention search))
        pathScan "/searchHashtag/%s" (fun search -> OK (searchHashtag search))
    ]

startWebServer defaultConfig app
