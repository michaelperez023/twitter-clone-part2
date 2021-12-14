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
  | RegisterNewUser of string
  | Follow of string * string
  | AddActiveUser of string * WebSocket
  | RemoveActiveUser of string
  | UpdateFeeds of string * string * string

let agent = MailboxProcessor<string*WebSocket>.Start(fun inbox ->
  let rec messageLoop() = async {
    let! msg,webSkt = inbox.Receive()
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
      | RegisterNewUser(username) ->
        usernameUsernamesFollowedSetMap <- Map.add username Set.empty usernameUsernamesFollowedSetMap
        feedtable <- Map.add username List.empty feedtable
      | Follow(username, followedUsername) ->
        if usernameUsernamesFollowedSetMap.ContainsKey followedUsername then
          let mutable followSet = Set.empty
          followSet <- usernameUsernamesFollowedSetMap.[followedUsername]
          followSet <- Set.add username followSet
          usernameUsernamesFollowedSetMap <- Map.add followedUsername followSet usernameUsernamesFollowedSetMap
          let mutable jsonData: ResponseType = 
            {username = followedUsername; service= "Follow"; code = "OK"; message = sprintf "User %s started following you!" username}
          let mutable consJson = Json.serialize jsonData
          agent.Post (consJson, activeUsers.[followedUsername])
      | AddActiveUser(username, userWebSkt) ->
        activeUsers <- Map.add username userWebSkt activeUsers 
        let mutable feedsPub = ""
        let mutable sertype = ""
        if feedtable.ContainsKey username then
          let mutable feedsTop = ""
          let mutable fSize = 10
          let feedList:List<string> = feedtable.[username]
          if feedList.Length = 0 then
            sertype <- "Follow"
            feedsPub <- sprintf "No feeds yet!!"
          else
            if feedList.Length < 10 then
                fSize <- feedList.Length
            // printfn"fsize = %d" fSize
            for i in [0..(fSize-1)] do
              // printfn "feed %d = %s" i feedtable.[username].[i]
              feedsTop <- "-" + feedtable.[username].[i] + feedsTop

            feedsPub <- feedsTop
            sertype <- "LiveFeed"
          // printfn "feeds pub = %A" feedsPub
          let jsonData: ResponseType = {username = username; message = feedsPub; code = "OK"; service = sertype}
          let consJson = Json.serialize jsonData
          agent.Post (consJson, userWebSkt)
      | RemoveActiveUser(username) ->
        activeUsers <- Map.remove username activeUsers
      | UpdateFeeds(username, tweetMsg, sertype) ->
        if usernameUsernamesFollowedSetMap.ContainsKey username then
          let mutable stype = ""
          if sertype = "Tweet" then
            stype <- sprintf "%s tweeted:" username
          for foll in usernameUsernamesFollowedSetMap.[username] do 
            if usernameUsernamesFollowedSetMap.ContainsKey foll then
              if activeUsers.ContainsKey foll then
                let twt = sprintf "%s^%s" stype tweetMsg
                let jsonData: ResponseType = {username = foll; service=sertype; code="OK"; message = twt}
                let consJson = Json.serialize jsonData
                agent.Post (consJson,activeUsers.[foll])
              let mutable listy = []
              if feedtable.ContainsKey foll then
                  listy <- feedtable.[foll]
              listy  <- (sprintf "%s^%s" stype tweetMsg) :: listy
              feedtable <- Map.add foll listy feedtable
      return! loop()
  }
  loop()

let feedActor = spawn system (sprintf "FeedActor") FeedActor

let liveFeed (webSocket : WebSocket) (context: HttpContext) =
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

let loginUser userInput =
  let mutable response = ""
  if usernamePasswordMap.ContainsKey userInput.username then
    if usernamePasswordMap.[userInput.username] = userInput.value then
      let rectype: ResponseType = {username = userInput.username; message = sprintf "User %s logged in" userInput.username; service = "Login"; code = "OK"}
      response <- rectype |> Json.toJson |> System.Text.Encoding.UTF8.GetString
    else 
      let rectype: ResponseType = {username = userInput.username; message = "Invalid username and/or password"; service = "Login"; code = "FAIL"}
      response <- rectype |> Json.toJson |> System.Text.Encoding.UTF8.GetString
  else
    let rectype: ResponseType = {username = userInput.username; message = "Invalid username and/or password"; service = "Login"; code = "FAIL"}
    response <- rectype |> Json.toJson |> System.Text.Encoding.UTF8.GetString
  response

let registerUser userInput =
  let mutable response = ""
  if usernamePasswordMap.ContainsKey userInput.username then
    let rectype: ResponseType = {username = userInput.username; message = sprintf "User %s is already registered" userInput.username; service = "Register"; code = "FAIL"}
    response <- rectype |> Json.toJson |> System.Text.Encoding.UTF8.GetString
  else
    usernamePasswordMap <- Map.add userInput.username userInput.value usernamePasswordMap
    usernameFollowersSetMap <- Map.add userInput.username Set.empty usernameFollowersSetMap
    feedActor <! RegisterNewUser(userInput.username)
    let rectype: ResponseType = {username = userInput.username; message = sprintf "User %s registered" userInput.username; service = "Register"; code = "OK"}
    response <- rectype |> Json.toJson |> System.Text.Encoding.UTF8.GetString
  response

let followUser userInput =
  let mutable resp = ""
  if userInput.value <> userInput.username then 
    if usernameFollowersSetMap.ContainsKey userInput.value then
      if not (usernameFollowersSetMap.[userInput.value].Contains userInput.username) then
        let mutable tempSet = usernameFollowersSetMap.[userInput.value]
        tempSet <- Set.add userInput.username tempSet
        usernameFollowersSetMap <- Map.add userInput.value tempSet usernameFollowersSetMap
        feedActor <! Follow(userInput.username, userInput.value) 
        let rectype: ResponseType = {username = userInput.username; service="Follow"; message = sprintf "You started following %s." userInput.value; code = "OK"}
        resp <- rectype |> Json.toJson |> System.Text.Encoding.UTF8.GetString
      else 
        let rectype: ResponseType = {username = userInput.username; service="Follow"; message = sprintf "You are already following user %s." userInput.value; code = "FAIL"}
        resp <- rectype |> Json.toJson |> System.Text.Encoding.UTF8.GetString      
    else  
      let rectype: ResponseType = {username = userInput.username; service="Follow"; message = sprintf "Invalid username, user %s is not registered." userInput.value; code = "FAIL"}
      resp <- rectype |> Json.toJson |> System.Text.Encoding.UTF8.GetString
  else
    let rectype: ResponseType = {username = userInput.username; service="Follow"; message = sprintf "You cannot follow yourself."; code = "FAIL"}
    resp <- rectype |> Json.toJson |> System.Text.Encoding.UTF8.GetString    
  resp

let tweetUser userInput =
  let mutable resp = ""
  if usernamePasswordMap.ContainsKey userInput.username then
    let mutable hashtag = ""
    let mutable mention = ""
    let parsed = userInput.value.Split ' '
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
        tempList <- (sprintf "%s tweeted:^%s" userInput.username userInput.value) :: tempList
        usernameMentionsListMap <- Map.add mention tempList usernameMentionsListMap
        feedActor <! UpdateFeeds(userInput.username, userInput.value, "Tweet")
        let rectype: ResponseType = {username = userInput.username; service="Tweet"; message = (sprintf "User %s tweeted:^%s" userInput.username userInput.value); code = "OK"}
        resp <- rectype |> Json.toJson |> System.Text.Encoding.UTF8.GetString
      else
        let rectype: ResponseType = {username = userInput.username; service="Tweet"; message = sprintf "Invalid tweet, user %s is not registered." mention; code = "FAIL"}
        resp <- rectype |> Json.toJson |> System.Text.Encoding.UTF8.GetString
    else
      feedActor <! UpdateFeeds(userInput.username, userInput.value, "Tweet")
      let rectype: ResponseType = {username = userInput.username; service="Tweet"; message = (sprintf "User %s tweeted:^%s" userInput.username userInput.value); code = "OK"}
      resp <- rectype |> Json.toJson |> System.Text.Encoding.UTF8.GetString

    if hashtag <> "" then
      if not (hashtagTweetsListMap.ContainsKey hashtag) then
        hashtagTweetsListMap <- Map.add hashtag List.empty hashtagTweetsListMap
      let mutable tempList = hashtagTweetsListMap.[hashtag]
      tempList <- (sprintf "User %s tweeted:^%s" userInput.username userInput.value) :: tempList
      hashtagTweetsListMap <- Map.add hashtag tempList hashtagTweetsListMap
  else
    let rectype: ResponseType = {username = userInput.username; service="Tweet"; message = sprintf "Invalid request by user %s, that user is not registered yet." userInput.username; code = "FAIL"}
    resp <- rectype |> Json.toJson |> System.Text.Encoding.UTF8.GetString
  resp

let query (userInput:string) =
  let mutable resp = ""
  let mutable hashtagsString = ""
  let mutable mentionsString = ""
  let mutable size = 10
  //printfn "userInput: %s" userInput
  if userInput.Length > 0 then
    if userInput.[0] = '@' then
      let searchKey = userInput.[1..(userInput.Length-1)]
      if usernameMentionsListMap.ContainsKey searchKey then
        let mapData:List<string> = usernameMentionsListMap.[searchKey]
        if (mapData.Length < 10) then
          size <- mapData.Length
        for i in [0..(size-1)] do
          mentionsString <- mentionsString + "-" + mapData.[i]
        let rectype: ResponseType = {username = ""; service="Query"; message = mentionsString; code = "OK"}
        resp <- Json.serialize rectype
      else
        let rectype: ResponseType = {username = ""; service="Query"; message = sprintf "No mentions found for user %s." searchKey; code = "FAIL"}
        resp <- Json.serialize rectype
    else //if userInput.[0] = '#' then
      let searchKey = userInput
      if hashtagTweetsListMap.ContainsKey searchKey then
        let mapData:List<string> = hashtagTweetsListMap.[searchKey]
        if (mapData.Length < 10) then
            size <- mapData.Length
        for i in [0..(size-1)] do
            hashtagsString <- hashtagsString + "-" + mapData.[i]
        let rectype: ResponseType = {username = ""; service="Query"; message = hashtagsString; code = "OK"}
        resp <- Json.serialize rectype
      else
        let rectype: ResponseType = {username = ""; service="Query"; message = sprintf "No tweets found for the hashtag %s."  searchKey; code = "FAIL"}
        resp <- Json.serialize rectype
    (*else
        let rectype: ResponseType = {username = ""; service="Query"; message = "Your query must begin with a # or @."; code = "OK"}
        resp <- Json.serialize rectype*)
  else
    let rectype: ResponseType = {username = ""; service="Query"; message = "Type something to search."; code = "FAIL"}
    resp <- Json.serialize rectype
  resp

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
  
  let entryDone userInput =
    let userRegResp = resource.Entry userInput
    userRegResp
  
  path resourcePath >=> POST >=> request (getReqResource >> entryDone >> respInJson)

let app =
  choose [
    path "/livefeed" >=> handShake liveFeed
    entryRequest "login" {Entry = loginUser}
    entryRequest "register" {Entry = registerUser}
    entryRequest "follow" {Entry = followUser}
    entryRequest "tweet" {Entry = tweetUser}
    pathScan "/search/%s"
      (fun searchkey ->
        let keyval = (sprintf "%s" searchkey)
        let reply = query keyval
        OK reply)
    GET >=> path "/" >=> Files.file "index.html"
  ]

startWebServer defaultConfig app
