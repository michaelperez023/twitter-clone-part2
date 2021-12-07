open System.IO
open Suave
open Suave.Filters
open Suave.Operators

let app : WebPart =
  choose [
    GET >=> choose [path "/" >=> Files.file "index.html"; Files.browseHome]
    //GET >=> path "/" >=> Files.file "index.html"
    //GET >=> Files.browseHome
  ]

startWebServer defaultConfig app