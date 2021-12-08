open System.IO
open Suave
open Suave.Filters
open Suave.Operators
open Newtonsoft.Json
open Newtonsoft.Json.Serialization

(*
type RequestType = {
  userID: string
  value: string
}

type RestResource<'a> = {
    Entry : RequestType -> string
}

let loginUser userInput =
  let mutable resp = "test"
  if registeredUsers.ContainsKey userInput.userID then
    if registeredUsers.[userInput.userID] = userInput.value then
      let rectype: ResponseType = {userID = userInput.userID; message = sprintf "User %s logged in successfully" userInput.userID; service = "Login"; code = "OK"}
      resp <- rectype |> Json.toJson |> System.Text.Encoding.UTF8.GetString
    else 
      let rectype: ResponseType = {userID = userInput.userID; message = "Invalid userid / password"; service = "Login"; code = "FAIL"}
      resp <- rectype |> Json.toJson |> System.Text.Encoding.UTF8.GetString
  else
    let rectype: ResponseType = {userID = userInput.userID; message = "Invalid userid / password"; service = "Login"; code = "FAIL"}
    resp <- rectype |> Json.toJson |> System.Text.Encoding.UTF8.GetString
  resp

let respInJson v =     
    let jsonSerializerSettings = JsonSerializerSettings()
    jsonSerializerSettings.ContractResolver <- CamelCasePropertyNamesContractResolver()
    JsonConvert.SerializeObject(v, jsonSerializerSettings) |> OK 
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
*)

let app =
  choose [
    //LoginUserPoint
    GET >=> choose [path "/" >=> Files.file "index.html"; Files.browseHome]
    //GET >=> path "/" >=> Files.file "index.html"
    //GET >=> Files.browseHome
  ]

startWebServer defaultConfig app