open Suave
open Suave.Filters
open Suave.Operators
open Suave.Successful
open Suave.Sockets
open Suave.WebSocket
open Suave.Sockets.Control
open Suave.RequestErrors
open Newtonsoft.Json
open Newtonsoft.Json.Serialization
open Akka.FSharp
open Akka.Actor
open FSharp.Json

let system = ActorSystem.Create("TwitterServer")

type ResponseType = {
  username: string
  message: string
  service: string
  code: string
}

type RequestType = {
  username: string
  value: string
}

type RestResource<'a> = {
    Entry : RequestType -> string
}

type FeedActorMessages =
  | RegisterUser of string
  | FollowUser of string * string
  | AddActiveUser of string * WebSocket
  | RemoveActiveUser of string
  | UpdateFeed of string * string * string

let agent = MailboxProcessor<string * WebSocket>.Start(fun inbox ->
  let rec messageLoop() = async {
    let! msg, webSkt = inbox.Receive()
    let byteRes =
      msg
      |> System.Text.Encoding.ASCII.GetBytes
      |> ByteSegment
    let! _ = webSkt.send Text byteRes true
    return! messageLoop()
  }
  messageLoop()
)

let FeedActor (mailbox:Actor<_>) = 
  let mutable usernameUsernamesFollowedSetMap = Map.empty
  let mutable activeUsers = Map.empty
  let mutable feedtable = Map.empty
  let rec loop () = actor {
      let! message = mailbox.Receive()
      match message with
      | RegisterUser(username) ->
        usernameUsernamesFollowedSetMap <- Map.add username Set.empty usernameUsernamesFollowedSetMap
        feedtable <- Map.add username List.empty feedtable
      | FollowUser(username, followedUsername) ->
        if usernameUsernamesFollowedSetMap.ContainsKey followedUsername then
          let mutable followSet = Set.empty
          followSet <- usernameUsernamesFollowedSetMap.[followedUsername]
          followSet <- Set.add username followSet
          usernameUsernamesFollowedSetMap <- Map.add followedUsername followSet usernameUsernamesFollowedSetMap
          let mutable jsonData: ResponseType =
            {username = followedUsername; service= "Follow"; code = "OK"; message = "User " + username + " started following you."}
          let mutable consJson = Json.serialize jsonData
          agent.Post (consJson, activeUsers.[followedUsername])
      | AddActiveUser(username, userWebSocket) ->
        activeUsers <- Map.add username userWebSocket activeUsers
        let mutable feedsPub = ""
        let mutable serviceType = ""
        if feedtable.ContainsKey username then
          let mutable feedsTop = ""
          let mutable feedLength = 10
          let feedList:List<string> = feedtable.[username]
          if feedList.Length = 0 then
            serviceType <- "Follow"
            feedsPub <- "No tweets in feed yet."
          else
            if feedList.Length < 10 then
                feedLength <- feedList.Length
            // printfn"fsize = %d" fSize
            for i in [0..(feedLength-1)] do
              // printfn "feed %d = %s" i feedtable.[username].[i]
              feedsTop <- "-" + feedtable.[username].[i] + feedsTop

            feedsPub <- feedsTop
            serviceType <- "Feed"
          // printfn "feeds pub = %A" feedsPub
          let jsonData: ResponseType = {username = username; message = feedsPub; code = "OK"; service = serviceType}
          let consJson = Json.serialize jsonData
          agent.Post (consJson, userWebSocket)
      | RemoveActiveUser(username) ->
        activeUsers <- Map.remove username activeUsers
      | UpdateFeed(username, tweetMsg, serviceType) ->
        if usernameUsernamesFollowedSetMap.ContainsKey username then
          let mutable stype = ""
          if serviceType = "Tweet" then
            stype <- username + " tweeted:"
          for user in usernameUsernamesFollowedSetMap.[username] do
            if usernameUsernamesFollowedSetMap.ContainsKey user then
              if activeUsers.ContainsKey user then
                let twt = stype + "^" + tweetMsg
                let jsonData: ResponseType = {username = user; service=serviceType; code="OK"; message = twt}
                let consJson = Json.serialize jsonData
                agent.Post (consJson, activeUsers.[user])
              let mutable listy = []
              if feedtable.ContainsKey user then
                  listy <- feedtable.[user]
              listy  <- (stype + "^" + tweetMsg) :: listy
              feedtable <- Map.add user listy feedtable
      return! loop()
  }
  loop()

let feedActor = spawn system ("FeedActor") FeedActor

let feed (webSocket : WebSocket) (context: HttpContext) =
  let rec loop() =
    let mutable user = ""
    socket { 
      let! msg = webSocket.read()
      match msg with
      | (Text, data, true) ->
        let reqMsg = UTF8.toString data
        let parsed = Json.deserialize<RequestType> reqMsg
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
      let rectype: ResponseType = {username = input.username; message = "User " + input.username + " logged in."; service = "Login"; code = "OK"}
      response <- rectype |> Json.toJson |> System.Text.Encoding.UTF8.GetString
    else 
      let rectype: ResponseType = {username = input.username; message = "Invalid username and/or password."; service = "Login"; code = "FAIL"}
      response <- rectype |> Json.toJson |> System.Text.Encoding.UTF8.GetString
  else
    let rectype: ResponseType = {username = input.username; message = "Invalid username and/or password."; service = "Login"; code = "FAIL"}
    response <- rectype |> Json.toJson |> System.Text.Encoding.UTF8.GetString
  response

let registerUser input =
  let mutable response = ""
  if not (usernamePasswordMap.ContainsKey input.username) then
    usernamePasswordMap <- Map.add input.username input.value usernamePasswordMap
    usernameFollowersSetMap <- Map.add input.username Set.empty usernameFollowersSetMap
    feedActor <! RegisterUser(input.username)
    let rectype: ResponseType = {username = input.username; message = "User " + input.username + " registered."; service = "Register"; code = "OK"}
    response <- rectype |> Json.toJson |> System.Text.Encoding.UTF8.GetString
  else
    let rectype: ResponseType = {username = input.username; message = "User " + input.username + " is already registered."; service = "Register"; code = "FAIL"}
    response <- rectype |> Json.toJson |> System.Text.Encoding.UTF8.GetString
  response

let followUser input =
  let mutable response = ""
  if input.value <> input.username then
    if usernameFollowersSetMap.ContainsKey input.value then
      if not (usernameFollowersSetMap.[input.value].Contains input.username) then
        let mutable tempSet = usernameFollowersSetMap.[input.value]
        tempSet <- Set.add input.username tempSet
        usernameFollowersSetMap <- Map.add input.value tempSet usernameFollowersSetMap
        feedActor <! FollowUser(input.username, input.value)
        let rectype: ResponseType = {username = input.username; service="Follow"; message = "You started following " + input.value + "."; code = "OK"}
        response <- rectype |> Json.toJson |> System.Text.Encoding.UTF8.GetString
      else
        let rectype: ResponseType = {username = input.username; service="Follow"; message = "You are already following user " + input.value + "."; code = "FAIL"}
        response <- rectype |> Json.toJson |> System.Text.Encoding.UTF8.GetString
    else
      let rectype: ResponseType = {username = input.username; service="Follow"; message = "Invalid username, user " + input.value + " is not registered."; code = "FAIL"}
      response <- rectype |> Json.toJson |> System.Text.Encoding.UTF8.GetString
  else
    let rectype: ResponseType = {username = input.username; service="Follow"; message = "You cannot follow yourself."; code = "FAIL"}
    response <- rectype |> Json.toJson |> System.Text.Encoding.UTF8.GetString    
  response

let tweetUser input =
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
        let mutable tempList = usernameMentionsListMap.[mention]
        tempList <- (input.username + " tweeted:^" + input.value) :: tempList
        usernameMentionsListMap <- Map.add mention tempList usernameMentionsListMap
        feedActor <! UpdateFeed(input.username, input.value, "Tweet")
        let rectype: ResponseType = {username = input.username; service="Tweet"; message = "User " + input.username + " tweeted:^" + input.value; code = "OK"}
        response <- rectype |> Json.toJson |> System.Text.Encoding.UTF8.GetString
      else
        let rectype: ResponseType = {username = input.username; service="Tweet"; message = "Invalid tweet, user " + mention + " is not registered."; code = "FAIL"}
        response <- rectype |> Json.toJson |> System.Text.Encoding.UTF8.GetString
    else
      feedActor <! UpdateFeed(input.username, input.value, "Tweet")
      let rectype: ResponseType = {username = input.username; service="Tweet"; message = "User " + input.username + " tweeted:^" + input.value; code = "OK"}
      response <- rectype |> Json.toJson |> System.Text.Encoding.UTF8.GetString

    if hashtag <> "" then
      if not (hashtagTweetsListMap.ContainsKey hashtag) then
        hashtagTweetsListMap <- Map.add hashtag List.empty hashtagTweetsListMap
      let mutable tempList = hashtagTweetsListMap.[hashtag]
      tempList <- ("User " + input.username + " tweeted:^" + input.value) :: tempList
      hashtagTweetsListMap <- Map.add hashtag tempList hashtagTweetsListMap
  else
    let rectype: ResponseType = {username = input.username; service="Tweet"; message = "Invalid request by user " + input.username + ", that user is not registered yet."; code = "FAIL"}
    response <- rectype |> Json.toJson |> System.Text.Encoding.UTF8.GetString
  response

let query (input:string) =
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
        let rectype: ResponseType = {username = ""; service="Query"; message = mentionsString; code = "OK"}
        response <- Json.serialize rectype
      else
        let rectype: ResponseType = {username = ""; service="Query"; message = "No mentions found for user " + searchKey + "."; code = "FAIL"}
        response <- Json.serialize rectype
    else //if input.[0] = '#' then
      let searchKey = input
      if hashtagTweetsListMap.ContainsKey searchKey then
        let mapData:List<string> = hashtagTweetsListMap.[searchKey]
        if (mapData.Length < 10) then
            size <- mapData.Length
        for i in [0..(size-1)] do
            hashtagsString <- hashtagsString + "-" + mapData.[i]
        let rectype: ResponseType = {username = ""; service="Query"; message = hashtagsString; code = "OK"}
        response <- Json.serialize rectype
      else
        let rectype: ResponseType = {username = ""; service="Query"; message = "No tweets found for the hashtag " + searchKey + "."; code = "FAIL"}
        response <- Json.serialize rectype
    (*else
        let rectype: ResponseType = {username = ""; service="Query"; message = "Your query must begin with a # or @."; code = "OK"}
        response <- Json.serialize rectype*)
  else
    let rectype: ResponseType = {username = ""; service="Query"; message = "Type something to search."; code = "FAIL"}
    response <- Json.serialize rectype
  response

let respInJson v =
  let jsonSerializerSettings = JsonSerializerSettings()
  JsonConvert.SerializeObject(v, jsonSerializerSettings)
  |> OK 
  >=> Writers.setMimeType "application/json; charset=utf-8"

let fromJson<'a> json = JsonConvert.DeserializeObject(json, typeof<'a>) :?> 'a   

let getReqResource<'a> (requestInp : HttpRequest) = 
  let getInString (rawForm:byte[]) = System.Text.Encoding.UTF8.GetString(rawForm)
  let requestArr:byte[] = requestInp.rawForm
  requestArr |> getInString |> fromJson<RequestType>

let entryRequest resourceName resource = 
  let resourcePath = "/" + resourceName
  
  let entryDone input =
    let userRegResp = resource.Entry input
    userRegResp
  
  path resourcePath >=> POST >=> request (getReqResource >> entryDone >> respInJson)

let app =
  choose [
    path "/feed" >=> handShake feed
    entryRequest "login" {Entry = loginUser}
    entryRequest "register" {Entry = registerUser}
    entryRequest "follow" {Entry = followUser}
    entryRequest "tweet" {Entry = tweetUser}
    pathScan "/search/%s"
      (fun searchkey ->
        let keyval = searchkey
        let reply = query keyval
        OK reply)
    GET >=> path "/" >=> Files.file "index.html"
  ]

startWebServer defaultConfig app
