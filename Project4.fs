open Suave
open Suave.Filters
open Suave.Operators
open Suave.Successful
open Newtonsoft.Json
open Newtonsoft.Json.Serialization
open Akka.FSharp
open Akka.Actor

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

let mutable usernamePasswordMap = Map.empty
//let mutable usernameFollowersSetMap = Map.empty // TODO setup

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

let regNewUser userInput =
  let mutable response = ""
  if usernamePasswordMap.ContainsKey userInput.username then
    let rectype: ResponseType = {username = userInput.username; message = sprintf "User %s already registered" userInput.username; service = "Register"; code = "FAIL"}
    response <- rectype |> Json.toJson |> System.Text.Encoding.UTF8.GetString
  else
    usernamePasswordMap <- Map.add userInput.username userInput.value usernamePasswordMap
    //usernameFollowersSetMap <- Map.add userInput.username Set.empty usernameFollowersSetMap // TODO
    //feedActor <! RegisterNewUser(userInput.username) // TODO
    let rectype: ResponseType = {username = userInput.username; message = sprintf "User %s registered successfully" userInput.username; service = "Register"; code = "OK"}
    response <- rectype |> Json.toJson |> System.Text.Encoding.UTF8.GetString
  response

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
let RegisterNewUserPoint = entryRequest "register" {Entry = regNewUser}

let app =
  choose [
    LoginUserPoint
    RegisterNewUserPoint
    GET >=> choose [path "/" >=> Files.file "index.html"; Files.browseHome]
  ]

startWebServer defaultConfig app