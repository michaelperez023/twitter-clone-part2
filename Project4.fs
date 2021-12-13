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
  | AddActiveUsers of string * WebSocket

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
          //let mutable jsonData: ResponseType = 
            //{username = followedUsername; service= "Follow"; code = "OK"; message = sprintf "User %s started following you!" username}
          //let mutable consJson = Json.serialize jsonData
          // printfn "consjaosn = %A" consJson
          // agent.Post (consJson, activeUsers.[followedUsername]) TODO
      | AddActiveUsers(username, userWebSkt) ->
        if activeUsers.ContainsKey username then  
          activeUsers <- Map.remove username activeUsers
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
          let jsonData: ResponseType = {username = username; message = feedsPub; code = "OK"; service=sertype}
          let consJson = Json.serialize jsonData
          agent.Post (consJson, userWebSkt) 
      return! loop()
  }
  loop()

let feedActor = spawn system (sprintf "FeedActor") FeedActor

let liveFeed (webSocket : WebSocket) (context: HttpContext) =
  let rec loop() =
    let mutable presentUser = ""
    socket { 
      let! msg = webSocket.read()
      match msg with
      | (Text, data, true) ->
        let reqMsg = UTF8.toString data
        let parsed = Json.deserialize<RequestType> reqMsg
        presentUser <- parsed.username
        //feedActor <! AddActiveUsers(parsed.username, webSocket)
        return! loop()
      | (Close, _, _) ->
        printfn "Closed WEBSOCKET"
        //feedActor <! RemoveActiveUsers(presentUser)
        let emptyResponse = [||] |> ByteSegment
        do! webSocket.send Close emptyResponse true
      | _ -> return! loop()
    }
  loop()

let mutable usernamePasswordMap = Map.empty
let mutable usernameFollowersSetMap = Map.empty // TODO setup

let loginUser userInput =
  let mutable response = ""
  if usernamePasswordMap.ContainsKey userInput.username then
    if usernamePasswordMap.[userInput.username] = userInput.value then
      let rectype: ResponseType = {username = userInput.username; message = sprintf "User %s logged in successfully" userInput.username; service = "Login"; code = "OK"}
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
    let rectype: ResponseType = {username = userInput.username; message = sprintf "User %s already registered" userInput.username; service = "Register"; code = "FAIL"}
    response <- rectype |> Json.toJson |> System.Text.Encoding.UTF8.GetString
  else
    usernamePasswordMap <- Map.add userInput.username userInput.value usernamePasswordMap
    usernameFollowersSetMap <- Map.add userInput.username Set.empty usernameFollowersSetMap
    feedActor <! RegisterNewUser(userInput.username)
    let rectype: ResponseType = {username = userInput.username; message = sprintf "User %s registered successfully" userInput.username; service = "Register"; code = "OK"}
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
        let rectype: ResponseType = {username = userInput.username; service="Follow"; message = sprintf "You started following %s!" userInput.value; code = "OK"}
        resp <- rectype |> Json.toJson |> System.Text.Encoding.UTF8.GetString
      else 
        let rectype: ResponseType = {username = userInput.username; service="Follow"; message = sprintf "You are already following %s!" userInput.value; code = "FAIL"}
        resp <- rectype |> Json.toJson |> System.Text.Encoding.UTF8.GetString      
    else  
      let rectype: ResponseType = {username = userInput.username; service="Follow"; message = sprintf "Invalid request, No such user (%s)." userInput.value; code = "FAIL"}
      resp <- rectype |> Json.toJson |> System.Text.Encoding.UTF8.GetString
  else
    let rectype: ResponseType = {username = userInput.username; service="Follow"; message = sprintf "You cannot follow yourself."; code = "FAIL"}
    resp <- rectype |> Json.toJson |> System.Text.Encoding.UTF8.GetString    
  resp

let respInJson v =
    let jsonSerializerSettings = JsonSerializerSettings()
    jsonSerializerSettings.ContractResolver <- CamelCasePropertyNamesContractResolver()
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
  
  choose [
    path resourcePath >=> choose [
      POST >=> request (getReqResource >> entryDone >> respInJson)
    ]
  ]

let LoginUserPoint = entryRequest "login" {Entry = loginUser}
let RegisterUserPoint = entryRequest "register" {Entry = registerUser}
let FollowUserPoint = entryRequest "follow" {Entry = followUser}

let app =
  choose [
    path "/livefeed" >=> handShake liveFeed
    LoginUserPoint
    RegisterUserPoint
    FollowUserPoint
    GET >=> choose [path "/" >=> Files.file "index.html"; Files.browseHome]
  ]

startWebServer defaultConfig app
