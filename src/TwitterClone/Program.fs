// Twitter clone, Part II: entry point. Starts the server and runs it until Ctrl+C.
// Authors: Michael Perez and Blas Kojusner

module TwitterClone.Program

open System
open System.Globalization
open System.IO
open System.Net
open System.Runtime.InteropServices
open System.Threading

let private usage =
    """Usage: TwitterClone [--port <port>] [--host <address>]

Starts the Twitter clone web server. Open the URL it prints in a browser.

Options:
  --port <port>      TCP port to listen on (default 8080)
  --host <address>   IP address to listen on (default 127.0.0.1; 0.0.0.0 listens on all interfaces)
  --help             Show this message"""

type private Command =
    | Run of Server.Settings
    | ShowUsage
    | Invalid of message: string

let private parseHost (value: string) =
    match value with
    | "localhost" -> Some IPAddress.Loopback
    | _ ->
        // IPAddress.TryParse also takes forms like "1" (0.0.0.1), so require a full address
        match IPAddress.TryParse value with
        | true, address when value.Contains ':' || value.Split('.').Length = 4 -> Some address
        | _ -> None

let rec private parse (settings: Server.Settings) (args: string list) =
    match args with
    | [] -> Run settings
    | ("--help" | "-h") :: _ -> ShowUsage
    | "--port" :: value :: rest ->
        match Int32.TryParse(value, NumberStyles.None, CultureInfo.InvariantCulture) with
        | true, port when port >= 1 && port <= 65535 -> parse { settings with Port = port } rest
        | _ -> Invalid(sprintf "Invalid port: %s (use a number from 1 to 65535)." value)
    | "--host" :: value :: rest ->
        match parseHost value with
        | Some address -> parse { settings with Host = address } rest
        | None -> Invalid(sprintf "Invalid host: %s (use an IP address such as 127.0.0.1)." value)
    | [ ("--port" | "--host") as option ] -> Invalid(sprintf "%s needs a value." option)
    | other :: _ -> Invalid(sprintf "Unknown argument: %s" other)

/// The exception inside the wrappers that tasks add, such as the SocketException for a port in use.
let rec private rootCause (ex: exn) =
    match ex with
    | :? AggregateException as aggregate when aggregate.InnerExceptions.Count = 1 -> rootCause aggregate.InnerExceptions[0]
    | _ -> ex

[<EntryPoint>]
let main argv =
    match parse Server.defaultSettings (List.ofArray argv) with
    | ShowUsage ->
        printfn "%s" usage
        0
    | Invalid message ->
        eprintfn "%s\n\n%s" message usage
        2
    | Run settings ->
        // Suave prints messages of its own: a stack trace when the port is taken, which is reported
        // below in one line, and a line each time the server stops. Leave the console to this program.
        Suave.Globals.messageWriter <- Some TextWriter.Null

        use stopRequested = new ManualResetEventSlim(false)

        let onSignal (context: PosixSignalContext) =
            // Stop cleanly instead of letting the runtime end the process
            context.Cancel <- true
            stopRequested.Set()

        use _interrupt = PosixSignalRegistration.Create(PosixSignal.SIGINT, onSignal)
        use _terminate = PosixSignalRegistration.Create(PosixSignal.SIGTERM, onSignal)

        let server =
            try
                Ok(Server.start settings)
            with ex ->
                Error((rootCause ex).Message)

        match server with
        | Error message ->
            eprintfn "Could not start the server on %O port %d: %s" settings.Host settings.Port message
            1
        | Ok server ->
            printfn "Twitter clone listening on %s (press Ctrl+C to stop)" server.Url

            if Server.listensOnAllInterfaces settings.Host then
                printfn "It listens on all network interfaces, so other devices can use this computer's IP address with port %d." settings.Port

            stopRequested.Wait()
            printfn "Stopping..."
            server.Stop()
            0
