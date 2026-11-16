module TwitterClone.Tests.DomainTests

open System
open System.Text
open Xunit
open TwitterClone.Domain

// A low iteration count keeps the tests fast; it does not change how hashing works
let private storedPassword = hashPassword 1_000 "password"

let private time = DateTimeOffset(2026, 11, 16, 12, 0, 0, TimeSpan.Zero)

let private ok result =
    match result with
    | Ok value -> value
    | Error error -> failwithf "Expected Ok, got %A" error

/// The error of a result, or None if it succeeded.
let private errorOf result =
    match result with
    | Ok _ -> None
    | Error error -> Some error

let private withUsers names =
    names |> List.fold (fun state name -> register name storedPassword state |> ok) emptyState

let private post author text state =
    postTweet author text time state |> ok

let private texts (tweets: Tweet list) = tweets |> List.map _.Text

// ---------------------------------------------------------------------------------------------
// Users and passwords
// ---------------------------------------------------------------------------------------------

[<Theory>]
[<InlineData("alice")>]
[<InlineData("Bob_2")>]
[<InlineData("x")>]
[<InlineData("fifteen_chars_1")>]
let ``valid usernames are accepted`` (name: string) =
    Assert.True(isValidUsername name)
    Assert.True(register name storedPassword emptyState |> Result.isOk)

[<Theory>]
[<InlineData("")>]
[<InlineData("sixteen_chars_12")>]
[<InlineData("two words")>]
[<InlineData("dash-name")>]
[<InlineData("@alice")>]
[<InlineData("alice\n")>]
[<InlineData("élodie")>]
let ``invalid usernames are rejected`` (name: string) =
    Assert.False(isValidUsername name)
    Assert.Equal(Some InvalidUsername, register name storedPassword emptyState |> errorOf)
    Assert.Equal(Error InvalidUsername, checkCredentials name "password")

[<Fact>]
let ``a missing username is rejected`` () =
    Assert.False(isValidUsername null)
    Assert.Equal(Error InvalidUsername, checkCredentials null "password")

[<Fact>]
let ``passwords are 1 to 128 characters long`` () =
    Assert.True(isValidPassword "p")
    Assert.True(isValidPassword (String('p', 128)))
    Assert.False(isValidPassword "")
    Assert.False(isValidPassword null)
    Assert.False(isValidPassword (String('p', 129)))
    // Counted in characters, as tweets are: a character outside the Basic Multilingual Plane counts once
    Assert.True(isValidPassword (String.replicate 128 "\U0001F600"))
    Assert.False(isValidPassword (String.replicate 129 "\U0001F600"))
    Assert.Equal(Error InvalidPassword, checkCredentials "alice" "")
    Assert.Equal(Ok(), checkCredentials "alice" "correct horse")

[<Fact>]
let ``a username can be registered only once, and usernames are case-sensitive`` () =
    let state = withUsers [ "alice" ]
    Assert.Equal(Some(UsernameTaken "alice"), register "alice" storedPassword state |> errorOf)
    Assert.True(register "Alice" storedPassword state |> Result.isOk)

[<Fact>]
let ``a password hash accepts the password and nothing else`` () =
    let stored = hashPassword 1_000 "correct horse"
    Assert.True(verifyPassword "correct horse" stored)
    Assert.False(verifyPassword "correct horse " stored)
    Assert.False(verifyPassword "Correct horse" stored)
    Assert.False(verifyPassword "" stored)
    Assert.Equal(16, stored.Salt.Length)
    Assert.Equal(32, stored.Hash.Length)
    Assert.Equal(1_000, stored.Iterations)
    Assert.NotEqual<byte[]>(Encoding.UTF8.GetBytes "correct horse", stored.Hash)

[<Fact>]
let ``the same password gets a new salt and hash each time`` () =
    let first = hashPassword 1_000 "secret"
    let second = hashPassword 1_000 "secret"
    Assert.NotEqual<byte[]>(first.Salt, second.Salt)
    Assert.NotEqual<byte[]>(first.Hash, second.Hash)
    Assert.True(verifyPassword "secret" first)
    Assert.True(verifyPassword "secret" second)

// ---------------------------------------------------------------------------------------------
// Following
// ---------------------------------------------------------------------------------------------

[<Fact>]
let ``follow and unfollow follow the rules`` () =
    let state = withUsers [ "alice"; "bob" ]
    Assert.Equal(Some CannotFollowSelf, follow "alice" "alice" state |> errorOf)
    Assert.Equal(Some(UnknownUser "carol"), follow "alice" "carol" state |> errorOf)
    Assert.Equal(Some InvalidUsername, follow "alice" "not a name" state |> errorOf)
    Assert.Equal(Some(NotFollowing "bob"), unfollow "alice" "bob" state |> errorOf)

    let following = follow "alice" "bob" state |> ok
    Assert.True(isFollowing "alice" "bob" following)
    Assert.False(isFollowing "bob" "alice" following)
    Assert.Equal(Some(AlreadyFollowing "bob"), follow "alice" "bob" following |> errorOf)
    Assert.Equal(Some CannotUnfollowSelf, unfollow "alice" "alice" following |> errorOf)
    Assert.Equal(Some(UnknownUser "carol"), unfollow "alice" "carol" following |> errorOf)

    let unfollowed = unfollow "alice" "bob" following |> ok
    Assert.False(isFollowing "alice" "bob" unfollowed)
    Assert.True(follow "alice" "bob" unfollowed |> Result.isOk)

// ---------------------------------------------------------------------------------------------
// Tweets
// ---------------------------------------------------------------------------------------------

[<Fact>]
let ``every hashtag and mention is found, without trailing punctuation`` () =
    let text =
        "Lunch with @alice, @bob and @alice again! #food #Gators. #FOOD (#weekend) mail: carol@example.com a#b"

    Assert.Equal<string list>([ "food"; "gators"; "weekend" ], hashtagsIn text)
    Assert.Equal<string list>([ "alice"; "bob" ], mentionsIn text)

[<Fact>]
let ``a hashtag or mention that runs into other letters does not count`` () =
    // Names use the letters A-Z and a-z only, so #café is not cut short to #caf, and @bobé is not bob
    let text =
        "Coffee #café, #naïve, #café and #tag_é at #cafe_2! Ask @bobé or é@bob, not @bob."

    Assert.Equal<string list>([ "cafe_2" ], hashtagsIn text)
    Assert.Equal<string list>([ "bob" ], mentionsIn text)

[<Fact>]
let ``a tweet is indexed under each of its hashtags and mentions`` () =
    let state = withUsers [ "alice"; "bob"; "carol" ]
    let state, tweet = post "alice" "Hi @bob and @carol! #one #two" state

    Assert.Equal<string list>([ tweet.Text ], searchHashtags "one" state |> ok |> texts)
    Assert.Equal<string list>([ tweet.Text ], searchHashtags "two" state |> ok |> texts)
    Assert.Equal<string list>([ tweet.Text ], searchMentions "bob" state |> ok |> texts)
    Assert.Equal<string list>([ tweet.Text ], searchMentions "carol" state |> ok |> texts)
    Assert.Empty(searchMentions "alice" state |> ok)

[<Fact>]
let ``a tweet that mentions an unregistered user is rejected`` () =
    let state = withUsers [ "alice"; "bob" ]
    Assert.Equal(Some(UnregisteredMention "zed"), postTweet "alice" "Hi @bob and @zed #news" time state |> errorOf)
    Assert.Equal("@zed is not registered.", describe (UnregisteredMention "zed"))

[<Fact>]
let ``tweets are trimmed and must be 1 to 280 characters long`` () =
    let state = withUsers [ "alice" ]
    Assert.Equal(Some EmptyTweet, postTweet "alice" "  \n\t " time state |> errorOf)
    Assert.Equal(Some EmptyTweet, postTweet "alice" null time state |> errorOf)
    Assert.Equal(Some(TweetTooLong 281), postTweet "alice" (String('a', 281)) time state |> errorOf)

    let _, longest = post "alice" ("  " + String('a', 280) + "\n") state
    Assert.Equal(String('a', 280), longest.Text)

    // A character outside the Basic Multilingual Plane counts once, as it does on the page
    let emoji = String.replicate 280 "\U0001F600"
    Assert.True(postTweet "alice" emoji time state |> Result.isOk)
    Assert.Equal(Some(TweetTooLong 281), postTweet "alice" (emoji + "!") time state |> errorOf)

[<Fact>]
let ``tweet ids go up by one`` () =
    let state = withUsers [ "alice" ]
    let state, first = post "alice" "one" state
    let _, second = post "alice" "two" state
    Assert.Equal(1L, first.Id)
    Assert.Equal(2L, second.Id)

[<Fact>]
let ``a tweet goes to the author's feed and the followers' feeds only`` () =
    // alice and carol follow bob; bob follows dave
    let state =
        withUsers [ "alice"; "bob"; "carol"; "dave" ]
        |> follow "alice" "bob"
        |> ok
        |> follow "carol" "bob"
        |> ok
        |> follow "bob" "dave"
        |> ok

    let state, tweet = post "bob" "Hello, followers" state

    for user in [ "alice"; "bob"; "carol" ] do
        Assert.Equal<Tweet list>([ tweet ], feedOf user state)

    Assert.Empty(feedOf "dave" state)

[<Fact>]
let ``after an unfollow, new tweets no longer reach the old follower`` () =
    let state = withUsers [ "alice"; "bob" ] |> follow "alice" "bob" |> ok
    let state, first = post "bob" "first" state
    let state = unfollow "alice" "bob" state |> ok
    let state, _ = post "bob" "second" state
    Assert.Equal<Tweet list>([ first ], feedOf "alice" state)
    Assert.Equal<string list>([ "second"; "first" ], feedOf "bob" state |> texts)

[<Fact>]
let ``lists keep their 100 most recent tweets, and results show the 10 most recent`` () =
    let state = withUsers [ "alice"; "bob" ] |> follow "alice" "bob" |> ok

    let state =
        [ 1..105 ]
        |> List.fold (fun state n -> post "bob" (sprintf "tweet %d #busy @alice" n) state |> fst) state

    for list in [ state.Feeds["alice"]; state.Feeds["bob"]; state.Hashtags["busy"]; state.Mentions["alice"] ] do
        Assert.Equal(100, list.Length)
        Assert.Equal("tweet 105 #busy @alice", list.Head.Text)
        Assert.Equal("tweet 6 #busy @alice", (List.last list).Text)

    let newestTen = [ 105..-1..96 ] |> List.map (sprintf "tweet %d #busy @alice")
    Assert.Equal<string list>(newestTen, feedOf "alice" state |> texts)
    Assert.Equal<string list>(newestTen, searchHashtags "busy" state |> ok |> texts)
    Assert.Equal<string list>(newestTen, searchMentions "alice" state |> ok |> texts)

// ---------------------------------------------------------------------------------------------
// Searches
// ---------------------------------------------------------------------------------------------

[<Fact>]
let ``searches ignore the leading at sign or hash, surrounding spaces and the case of hashtags`` () =
    let state = withUsers [ "alice"; "bob" ]
    let state, tweet = post "alice" "Go @bob #FSharp" state

    for term in [ "fsharp"; "#fsharp"; "  #FSharp "; "##FSHARP" ] do
        Assert.Equal<Tweet list>([ tweet ], searchHashtags term state |> ok)

    for term in [ "bob"; "@bob"; " @bob " ] do
        Assert.Equal<Tweet list>([ tweet ], searchMentions term state |> ok)

    // Usernames are case-sensitive
    Assert.Empty(searchMentions "Bob" state |> ok)
    Assert.Empty(searchHashtags "#csharp" state |> ok)
    Assert.Equal(Some EmptySearch, searchMentions " @ " state |> errorOf)
    Assert.Equal(Some EmptySearch, searchHashtags "#" state |> errorOf)
    Assert.Equal(Some EmptySearch, searchHashtags null state |> errorOf)
