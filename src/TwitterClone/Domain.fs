// Twitter clone, Part II: the data and the rules of the engine, as pure functions.
// Nothing here knows about actors or the web server, so it can be tested on its own.
// Authors: Michael Perez and Blas Kojusner

module TwitterClone.Domain

open System
open System.Security.Cryptography
open System.Text.RegularExpressions

/// Usernames are 1 to 15 characters long.
[<Literal>]
let MaxUsernameLength = 15

/// Passwords are 1 to 128 characters long (Unicode code points).
[<Literal>]
let MaxPasswordLength = 128

/// Tweets are 1 to 280 characters long (Unicode code points, after trimming).
[<Literal>]
let MaxTweetLength = 280

/// Each feed, hashtag list and mention list keeps this many of its most recent tweets.
[<Literal>]
let KeptPerList = 100

/// Searches, and the feed sent when a page connects, return this many of the most recent tweets.
[<Literal>]
let ResultCount = 10

/// PBKDF2-HMAC-SHA256 iterations used for new passwords unless the server is told otherwise.
[<Literal>]
let DefaultPasswordIterations = 600_000

type Tweet =
    { Id: int64
      Author: string
      Text: string
      Time: DateTimeOffset }

/// A salted PBKDF2-HMAC-SHA256 hash of a password, with the iteration count it was made with.
type PasswordHash =
    { Salt: byte[]
      Hash: byte[]
      Iterations: int }

type DomainError =
    | InvalidUsername
    | InvalidPassword
    | UsernameTaken of username: string
    | UnknownUser of username: string
    | CannotFollowSelf
    | CannotUnfollowSelf
    | AlreadyFollowing of username: string
    | NotFollowing of username: string
    | EmptyTweet
    | TweetTooLong of length: int
    | UnregisteredMention of username: string
    | EmptySearch

/// A message for the user that explains the error.
let describe error =
    match error with
    | InvalidUsername -> sprintf "Usernames are 1 to %d letters (A-Z, a-z), digits or underscores." MaxUsernameLength
    | InvalidPassword -> sprintf "Passwords are 1 to %d characters long." MaxPasswordLength
    | UsernameTaken name -> sprintf "@%s is already registered." name
    | UnknownUser name -> sprintf "@%s is not registered." name
    | CannotFollowSelf -> "You cannot follow yourself."
    | CannotUnfollowSelf -> "You cannot unfollow yourself."
    | AlreadyFollowing name -> sprintf "You already follow @%s." name
    | NotFollowing name -> sprintf "You do not follow @%s." name
    | EmptyTweet -> "Type something to tweet."
    | TweetTooLong length -> sprintf "Tweets are at most %d characters long; this one has %d." MaxTweetLength length
    | UnregisteredMention name -> sprintf "@%s is not registered." name
    | EmptySearch -> "Type something to search for."

/// Everything the engine knows. Lists of tweets are newest first.
type State =
    { /// Password hash of each registered user
      Passwords: Map<string, PasswordHash>
      /// The followers of each registered user
      Followers: Map<string, Set<string>>
      /// Each user's feed: their own tweets and the tweets of the users they follow
      Feeds: Map<string, Tweet list>
      /// Tweets by hashtag, keyed by the lowercase hashtag without '#'
      Hashtags: Map<string, Tweet list>
      /// Tweets that mention each user
      Mentions: Map<string, Tweet list>
      LastTweetId: int64 }

let emptyState =
    { Passwords = Map.empty
      Followers = Map.empty
      Feeds = Map.empty
      Hashtags = Map.empty
      Mentions = Map.empty
      LastTweetId = 0L }

// ---------------------------------------------------------------------------------------------
// Users and passwords
// ---------------------------------------------------------------------------------------------

/// Length in Unicode code points, which is what the page counts as well: a character outside the
/// Basic Multilingual Plane, such as most emoji, counts once although it takes two UTF-16 chars.
let characterCount (text: string) =
    let mutable count = 0

    for _ in text.EnumerateRunes() do
        count <- count + 1

    count

let private isNameChar (c: char) = Char.IsAsciiLetterOrDigit c || c = '_'

let isValidUsername (name: string) =
    not (String.IsNullOrEmpty name) && name.Length <= MaxUsernameLength && String.forall isNameChar name

let isValidPassword (password: string) =
    not (String.IsNullOrEmpty password) && characterCount password <= MaxPasswordLength

/// Checks the form of a username and password, before any hashing is done.
let checkCredentials (username: string) (password: string) =
    if not (isValidUsername username) then Error InvalidUsername
    elif not (isValidPassword password) then Error InvalidPassword
    else Ok()

[<Literal>]
let private SaltBytes = 16

[<Literal>]
let private HashBytes = 32

/// Hashes a password with a new random salt.
let hashPassword (iterations: int) (password: string) =
    let salt = RandomNumberGenerator.GetBytes SaltBytes
    { Salt = salt
      Hash = Rfc2898DeriveBytes.Pbkdf2(password, salt, iterations, HashAlgorithmName.SHA256, HashBytes)
      Iterations = iterations }

/// Checks a password against a stored hash, in time that does not depend on where they differ.
let verifyPassword (password: string) (stored: PasswordHash) =
    let candidate =
        Rfc2898DeriveBytes.Pbkdf2(password, stored.Salt, stored.Iterations, HashAlgorithmName.SHA256, stored.Hash.Length)

    CryptographicOperations.FixedTimeEquals(ReadOnlySpan<byte>(candidate), ReadOnlySpan<byte>(stored.Hash))

let isRegistered (username: string) (state: State) =
    not (isNull username) && state.Passwords.ContainsKey username

let register (username: string) (password: PasswordHash) (state: State) =
    if not (isValidUsername username) then
        Error InvalidUsername
    elif isRegistered username state then
        Error(UsernameTaken username)
    else
        Ok
            { state with
                Passwords = Map.add username password state.Passwords
                Followers = Map.add username Set.empty state.Followers
                Feeds = Map.add username [] state.Feeds }

let passwordOf (username: string) (state: State) =
    if isRegistered username state then Some state.Passwords[username] else None

// ---------------------------------------------------------------------------------------------
// Following
// ---------------------------------------------------------------------------------------------

let followersOf (username: string) (state: State) =
    state.Followers |> Map.tryFind username |> Option.defaultValue Set.empty

let isFollowing (follower: string) (followed: string) (state: State) =
    (followersOf followed state).Contains follower

/// Checks a follow or unfollow request; selfError is the error for a user naming themself.
let private checkTarget selfError (user: string) (target: string) (state: State) =
    if not (isRegistered user state) then Error(UnknownUser user)
    elif target = user then Error selfError
    elif not (isValidUsername target) then Error InvalidUsername
    elif not (isRegistered target state) then Error(UnknownUser target)
    else Ok()

let follow (follower: string) (target: string) (state: State) =
    checkTarget CannotFollowSelf follower target state
    |> Result.bind (fun () ->
        if isFollowing follower target state then
            Error(AlreadyFollowing target)
        else
            Ok { state with Followers = Map.add target (Set.add follower (followersOf target state)) state.Followers })

let unfollow (follower: string) (target: string) (state: State) =
    checkTarget CannotUnfollowSelf follower target state
    |> Result.bind (fun () ->
        if not (isFollowing follower target state) then
            Error(NotFollowing target)
        else
            Ok { state with Followers = Map.add target (Set.remove follower (followersOf target state)) state.Followers })

/// The users whose feeds get a tweet by this author: the author and the author's followers.
let audienceOf (author: string) (state: State) = Set.add author (followersOf author state)

// ---------------------------------------------------------------------------------------------
// Tweets
// ---------------------------------------------------------------------------------------------

// A hashtag or mention is '#' or '@' and then letters A-Z or a-z, digits and underscores. It must
// not be part of a longer word: "bob@example.com" mentions nobody, and "#café" is not a hashtag
// (rather than being taken as "#caf"). Punctuation right after it is left out.
let private tagPattern (marker: char) =
    Regex(sprintf @"(?<![\p{L}\p{M}\p{N}_])%c([A-Za-z0-9_]+)(?![\p{L}\p{M}\p{N}_])" marker, RegexOptions.CultureInvariant)

let private hashtagPattern = tagPattern '#'

let private mentionPattern = tagPattern '@'

/// The distinct hashtags of a text, lowercase and without '#', in order of appearance.
let hashtagsIn (text: string) =
    hashtagPattern.Matches(text)
    |> Seq.map (fun m -> m.Groups[1].Value.ToLowerInvariant())
    |> Seq.distinct
    |> List.ofSeq

/// The distinct usernames a text mentions, without '@', in order of appearance.
let mentionsIn (text: string) =
    mentionPattern.Matches(text)
    |> Seq.map (fun m -> m.Groups[1].Value)
    |> Seq.distinct
    |> List.ofSeq

/// Adds a tweet to the front of the list under the key, keeping the KeptPerList most recent.
let private addTo (key: string) (tweet: Tweet) (lists: Map<string, Tweet list>) =
    let existing = lists |> Map.tryFind key |> Option.defaultValue []
    Map.add key (tweet :: existing |> List.truncate KeptPerList) lists

/// Posts a tweet: it goes into the feeds of the author and the author's followers, the list of
/// each hashtag it contains and the mention list of each user it mentions.
let postTweet (author: string) (text: string) (time: DateTimeOffset) (state: State) =
    let text = if isNull text then "" else text.Trim()
    let length = characterCount text

    if not (isRegistered author state) then
        Error(UnknownUser author)
    elif length = 0 then
        Error EmptyTweet
    elif length > MaxTweetLength then
        Error(TweetTooLong length)
    else
        let mentions = mentionsIn text

        match mentions |> List.tryFind (fun name -> not (isRegistered name state)) with
        | Some name -> Error(UnregisteredMention name)
        | None ->
            let tweet =
                { Id = state.LastTweetId + 1L
                  Author = author
                  Text = text
                  Time = time }

            let feeds = audienceOf author state |> Set.fold (fun lists owner -> addTo owner tweet lists) state.Feeds
            let hashtags = hashtagsIn text |> List.fold (fun lists tag -> addTo tag tweet lists) state.Hashtags
            let mentionLists = mentions |> List.fold (fun lists name -> addTo name tweet lists) state.Mentions

            Ok(
                { state with
                    Feeds = feeds
                    Hashtags = hashtags
                    Mentions = mentionLists
                    LastTweetId = tweet.Id },
                tweet
            )

/// The ResultCount most recent tweets in a user's feed.
let feedOf (username: string) (state: State) =
    state.Feeds |> Map.tryFind username |> Option.defaultValue [] |> List.truncate ResultCount

// ---------------------------------------------------------------------------------------------
// Searches
// ---------------------------------------------------------------------------------------------

/// A search term without surrounding spaces and without the leading '@' or '#'.
let searchTerm (prefix: char) (term: string) =
    if isNull term then "" else term.Trim().TrimStart(prefix).Trim()

let private mostRecent (key: string) (lists: Map<string, Tweet list>) =
    lists |> Map.tryFind key |> Option.defaultValue [] |> List.truncate ResultCount

/// The ResultCount most recent tweets that mention a user ("alice" or "@alice").
let searchMentions (term: string) (state: State) =
    match searchTerm '@' term with
    | "" -> Error EmptySearch
    | username -> Ok(mostRecent username state.Mentions)

/// The ResultCount most recent tweets with a hashtag ("fsharp" or "#FSharp"; case does not matter).
let searchHashtags (term: string) (state: State) =
    match searchTerm '#' term with
    | "" -> Error EmptySearch
    | tag -> Ok(mostRecent (tag.ToLowerInvariant()) state.Hashtags)
