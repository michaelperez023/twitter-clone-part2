namespace TwitterClone

open WebSharper
open WebSharper.Sitelets
open WebSharper.UI
open WebSharper.UI.Server
open WebSharper.UI.Templating

type EndPoint =
    | [<EndPoint "/">] Home
    | [<EndPoint "/about">] About
    |  [<EndPoint "/login">] Login

module Templating =
    open WebSharper.UI.Html

    // Compute a menubar where the menu item for the given endpoint is active
    let MenuBar (ctx: Context<EndPoint>) endpoint : Doc list =
        let ( => ) txt act =
             li [if endpoint = act then yield attr.``class`` "active"] [
                a [attr.href (ctx.Link act)] [text txt]
             ]
        [
            "Home" => EndPoint.Home
            "About" => EndPoint.About
            "Login" => EndPoint.Login
        ]

    let Main ctx action (title: string) (body: Doc list) =
        Content.Page(
            Templates.MainTemplate()
                .Title(title)
                .MenuBar(MenuBar ctx action)
                .Body(body)
                .Doc()
        )

    (*let inputField (id: string) (which: string) (var: Var<string>) =
        Templates.MainTemplate()
            .Field()
            .Id(id)
            .Which(which)
            .Var(var)
            .Doc()
        let firstName = Var.Create ""
        let lastName = Var.Create ""
        Templates.MainTemplate()
            .MainAttr(attr.``class`` "my-form")
            .InputFields(
                [
                    inputField "first" "First" firstName
                    inputField "last" "Last" lastName
                ]
            )
            .Doc()*)

module Site =
    open WebSharper.UI.Html
    open WebSharper.JavaScript

    let HomePage ctx =
        Templating.Main ctx EndPoint.Home "Home" [
            h1 [] [text "Say Hi to the server!"]
            div [] [client <@ Client.Main() @>]
        ]

    let AboutPage ctx =
        Templating.Main ctx EndPoint.About "About" [
            h1 [] [text "About"]
            p [] [text "This is a template WebSharper client-server application."]
        ]
    
    let LoginPage ctx =
        Templating.Main ctx EndPoint.Login "Login" [
            h1 [] [text "Login"]
            p [] [text "This is a template WebSharper client-server application."]
            button [ on.click (fun el ev -> JS.Alert "Hi!") ] [ text "Click me!" ]
            //div [] [Templating.Main ]
            
            (*type MyTemplate = Template<"my-template.html">
            let username = Var.Create ""
            let myForm =
                let user = Var.Create ""
                MyTemplate()
                    .user(username)
                    .Doc()*)
        ]
        
    [<Website>]
    let Main =
        Application.MultiPage (fun ctx endpoint ->
            match endpoint with
            | EndPoint.Home -> HomePage ctx
            | EndPoint.About -> AboutPage ctx
            | EndPoint.Login -> LoginPage ctx
        )
