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

let system = ActorSystem.Create("TwitterServer")

type Response = {   // TODO edit
  username: string
  message: string
  service: string
  code: string
}

type Request = {   // TODO edit
  username: string
  value: string
}

type RestResource<'a> = {   // TODO edit
    Entry : Request -> string
}

type FeedActorMessages =
  | RegisterUserWithFeedActor of string
  | FollowUser of string * string
  | AddActiveUser of string * WebSocket
  | RemoveActiveUser of string
  | UpdateFeed of string * string * string

let socketMailboxProcessor = MailboxProcessor<string * WebSocket>.Start(fun inbox ->
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
  let mutable activeUsers = Map.empty
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
          let mutable jsonData: Response = {username = followedUsername; service= "Follow"; code = "OK"; message = "User " + username + " started following you."}
          socketMailboxProcessor.Post ((Json.serialize jsonData), activeUsers.[followedUsername])
      | AddActiveUser(username, userWebSocket) -> // TODO edit
        activeUsers <- Map.add username userWebSocket activeUsers
        let mutable feedsPub = ""
        let mutable serviceType = ""
        if usernameFeedListMap.ContainsKey username then
          let mutable feedsTop = ""
          let mutable feedLength = 10
          let feedList:List<string> = usernameFeedListMap.[username]
          if feedList.Length = 0 then
            serviceType <- "Follow"
            feedsPub <- "No tweets in feed yet."
          else
            if feedList.Length < 10 then
                feedLength <- feedList.Length
            // printfn"fsize = %d" fSize
            for i in [0..(feedLength-1)] do
              // printfn "feed %d = %s" i usernameFeedListMap.[username].[i]
              feedsTop <- "-" + usernameFeedListMap.[username].[i] + feedsTop

            feedsPub <- feedsTop
            serviceType <- "Feed"
          // printfn "feeds pub = %A" feedsPub
          let jsonData: Response = {username = username; message = feedsPub; code = "OK"; service = serviceType}
          let consJson = Json.serialize jsonData
          socketMailboxProcessor.Post (consJson, userWebSocket)
      | RemoveActiveUser(username) ->
        activeUsers <- Map.remove username activeUsers
      | UpdateFeed(username, tweetMsg, serviceType) ->  // TODO edit
        if usernameFollowersSetMap.ContainsKey username then
          let mutable stype = ""
          if serviceType = "Tweet" then
            stype <- "User " + username + " tweeted:"
          for user in usernameFollowersSetMap.[username] do
            if usernameFollowersSetMap.ContainsKey user then
              if activeUsers.ContainsKey user then
                let twt = stype + "^" + tweetMsg
                let jsonData: Response = {username = user; service=serviceType; code="OK"; message = twt}
                let consJson = Json.serialize jsonData
                socketMailboxProcessor.Post (consJson, activeUsers.[user])
              let mutable listy = []
              if usernameFeedListMap.ContainsKey user then
                  listy <- usernameFeedListMap.[user]
              listy  <- (stype + "^" + tweetMsg) :: listy
              usernameFeedListMap <- Map.add user listy usernameFeedListMap
      return! loop()
  }
  loop()

let feedActor = spawn system ("FeedActor") FeedActor

let feed (webSocket : WebSocket) (context: HttpContext) =  // TODO edit
  let rec loop() =
    let mutable user = ""
    socket { 
      let! msg = webSocket.read()
      match msg with
      | (Text, data, true) ->
        let reqMsg = UTF8.toString data
        let parsed = Json.deserialize<Request> reqMsg
        user <- parsed.username
        feedActor <! AddActiveUser(parsed.username, webSocket)
        return! loop()
      | (Close, _, _) ->
        feedActor <! RemoveActiveUser(user)
        let emptyResponse = [||] |> ByteSegment
        do! webSocket.send Close emptyResponse true
      | _ -> return! loop()
    }
  loop()

let mutable usernamePasswordMap = Map.empty
let mutable usernameFollowersSetMap = Map.empty
let mutable hashtagTweetsListMap = Map.empty
let mutable usernameMentionsListMap = Map.empty

let loginUser input =
  let mutable response = ""
  if usernamePasswordMap.ContainsKey input.username then
    if usernamePasswordMap.[input.username] = input.value then
      let rectype: Response = {username = input.username; message = "User " + input.username + " logged in."; service = "Login"; code = "OK"}
      response <- rectype |> Json.toJson |> System.Text.Encoding.UTF8.GetString
    else
      let rectype: Response = {username = input.username; message = "Invalid username and/or password."; service = "Login"; code = "FAIL"}
      response <- rectype |> Json.toJson |> System.Text.Encoding.UTF8.GetString
  else
    let rectype: Response = {username = input.username; message = "Invalid username and/or password."; service = "Login"; code = "FAIL"}
    response <- rectype |> Json.toJson |> System.Text.Encoding.UTF8.GetString
  response

let registerUser input =
  let mutable response = ""
  if not (usernamePasswordMap.ContainsKey input.username) then
    usernamePasswordMap <- Map.add input.username input.value usernamePasswordMap
    usernameFollowersSetMap <- Map.add input.username Set.empty usernameFollowersSetMap
    feedActor <! RegisterUserWithFeedActor(input.username)
    let rectype: Response = {username = input.username; message = "User " + input.username + " registered."; service = "Register"; code = "OK"}
    response <- rectype |> Json.toJson |> System.Text.Encoding.UTF8.GetString
  else
    let rectype: Response = {username = input.username; message = "User " + input.username + " is already registered."; service = "Register"; code = "FAIL"}
    response <- rectype |> Json.toJson |> System.Text.Encoding.UTF8.GetString
  response

let followUser input =
  let mutable response = ""
  if input.value <> input.username then
    if usernameFollowersSetMap.ContainsKey input.value then
      if not (usernameFollowersSetMap.[input.value].Contains input.username) then
        usernameFollowersSetMap <- Map.add input.value (Set.add input.username usernameFollowersSetMap.[input.value]) usernameFollowersSetMap
        feedActor <! FollowUser(input.username, input.value)
        let rectype: Response = {username = input.username; service="Follow"; message = "You started following user " + input.value + "."; code = "OK"}
        response <- rectype |> Json.toJson |> System.Text.Encoding.UTF8.GetString
      else
        let rectype: Response = {username = input.username; service="Follow"; message = "You are already following user " + input.value + "."; code = "FAIL"}
        response <- rectype |> Json.toJson |> System.Text.Encoding.UTF8.GetString
    else
      let rectype: Response = {username = input.username; service="Follow"; message = "Invalid username, user " + input.value + " is not registered."; code = "FAIL"}
      response <- rectype |> Json.toJson |> System.Text.Encoding.UTF8.GetString
  else
    let rectype: Response = {username = input.username; service="Follow"; message = "You cannot follow yourself."; code = "FAIL"}
    response <- rectype |> Json.toJson |> System.Text.Encoding.UTF8.GetString    
  response

let tweetUser input =  // TODO edit
  let mutable response = ""
  if usernamePasswordMap.ContainsKey input.username then
    let mutable hashtag = ""
    let mutable mention = ""
    let parsed = input.value.Split ' '
    // printfn "parsed = %A" parsed
    for parse in parsed do
      if parse.Length > 0 then
        if parse.[0] = '#' then
          hashtag <- parse.[1..(parse.Length-1)]
        else if parse.[0] = '@' then
          mention <- parse.[1..(parse.Length-1)]

    if mention <> "" then
      if usernamePasswordMap.ContainsKey mention then
        if not (usernameMentionsListMap.ContainsKey mention) then
            usernameMentionsListMap <- Map.add mention List.empty usernameMentionsListMap
        usernameMentionsListMap <- Map.add mention (("User " + input.username + " tweeted:^" + input.value) :: usernameMentionsListMap.[mention]) usernameMentionsListMap
        feedActor <! UpdateFeed(input.username, input.value, "Tweet")
        let rectype: Response = {username = input.username; service="Tweet"; message = "User " + input.username + " tweeted:^" + input.value; code = "OK"}
        response <- rectype |> Json.toJson |> System.Text.Encoding.UTF8.GetString
      else
        let rectype: Response = {username = input.username; service="Tweet"; message = "Invalid tweet, user " + mention + " is not registered."; code = "FAIL"}
        response <- rectype |> Json.toJson |> System.Text.Encoding.UTF8.GetString
    else
      feedActor <! UpdateFeed(input.username, input.value, "Tweet")
      let rectype: Response = {username = input.username; service="Tweet"; message = "User " + input.username + " tweeted:^" + input.value; code = "OK"}
      response <- rectype |> Json.toJson |> System.Text.Encoding.UTF8.GetString

    if hashtag <> "" then
      if not (hashtagTweetsListMap.ContainsKey hashtag) then
        hashtagTweetsListMap <- Map.add hashtag List.empty hashtagTweetsListMap
      hashtagTweetsListMap <- Map.add hashtag (("User " + input.username + " tweeted:^" + input.value) :: hashtagTweetsListMap.[hashtag]) hashtagTweetsListMap
  else
    let rectype: Response = {username = input.username; service="Tweet"; message = "Invalid request by user " + input.username + ", that user is not registered yet."; code = "FAIL"}
    response <- rectype |> Json.toJson |> System.Text.Encoding.UTF8.GetString
  response

let query (input:string) =  // TODO edit
  let mutable response = ""
  let mutable hashtagsString = ""
  let mutable mentionsString = ""
  let mutable size = 10
  //printfn "input: %s" input
  if input.Length > 0 then
    if input.[0] = '@' then
      let searchKey = input.[1..(input.Length-1)]
      if usernameMentionsListMap.ContainsKey searchKey then
        let mapData:List<string> = usernameMentionsListMap.[searchKey]
        if (mapData.Length < 10) then
          size <- mapData.Length
        for i in [0..(size-1)] do
          mentionsString <- mentionsString + "-" + mapData.[i]
        let rectype: Response = {username = ""; service="Query"; message = mentionsString; code = "OK"}
        response <- Json.serialize rectype
      else
        let rectype: Response = {username = ""; service="Query"; message = "No mentions found for user " + searchKey + "."; code = "FAIL"}
        response <- Json.serialize rectype
    else //if input.[0] = '#' then
      let searchKey = input
      if hashtagTweetsListMap.ContainsKey searchKey then
        let mapData:List<string> = hashtagTweetsListMap.[searchKey]
        if (mapData.Length < 10) then
            size <- mapData.Length
        for i in [0..(size-1)] do
            hashtagsString <- hashtagsString + "-" + mapData.[i]
        let rectype: Response = {username = ""; service="Query"; message = hashtagsString; code = "OK"}
        response <- Json.serialize rectype
      else
        let rectype: Response = {username = ""; service="Query"; message = "No tweets found for the hashtag " + searchKey + "."; code = "FAIL"}
        response <- Json.serialize rectype
    (*else
        let rectype: Response = {username = ""; service="Query"; message = "Your query must begin with a # or @."; code = "OK"}
        response <- Json.serialize rectype*)
  else
    let rectype: Response = {username = ""; service="Query"; message = "Type something to search."; code = "FAIL"}
    response <- Json.serialize rectype
  response

let entryRequest resourceName resource =
  let serialize input =
    JsonConvert.SerializeObject((resource.Entry (JsonConvert.DeserializeObject(System.Text.Encoding.UTF8.GetString(input.rawForm), typeof<Request>) :?> Request)), JsonSerializerSettings())
    |> OK

  path ("/" + resourceName) >=> POST >=> request(serialize)

let app =
  choose [
    GET >=> path "/" >=> Files.file "index.html"
    entryRequest "login" {Entry = loginUser}
    entryRequest "register" {Entry = registerUser}
    entryRequest "follow" {Entry = followUser}
    entryRequest "tweet" {Entry = tweetUser}
    path "/feed" >=> handShake feed
    pathScan "/search/%s" (fun searchKey -> OK (query searchKey))
  ]

startWebServer defaultConfig app
