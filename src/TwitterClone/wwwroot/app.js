// Twitter clone, Part II: the page's script. It calls the JSON API with fetch and gets the feed
// over a WebSocket. Text from users only ever goes into the page through textContent.
// Authors: Michael Perez and Blas Kojusner

'use strict';

(function () {
    const MAX_TWEET_LENGTH = 280;
    const MAX_PASSWORD_LENGTH = 128;
    const MAX_FEED_ITEMS = 100;
    const USERNAME_PATTERN = /^[A-Za-z0-9_]{1,15}$/;
    const USERNAME_RULE = 'Usernames are 1 to 15 letters (A-Z, a-z), digits or underscores.';
    const PASSWORD_RULE = 'Passwords are 1 to ' + MAX_PASSWORD_LENGTH + ' characters long.';
    // Seconds to wait before each try to reconnect the live feed; the last one repeats
    const RECONNECT_DELAYS = [1, 2, 5, 10, 30];
    // The WebSocket close code the server uses when the session is not valid
    const CLOSE_POLICY_VIOLATION = 1008;

    const byId = (id) => document.getElementById(id);

    const navMessage = byId('nav-message');
    const loginForm = byId('login-form');
    const loginUsername = byId('login-username');
    const loginPassword = byId('login-password');
    const loginMessage = byId('login-message');
    const registerForm = byId('register-form');
    const registerUsername = byId('register-username');
    const registerPassword = byId('register-password');
    const registerMessage = byId('register-message');
    const homeHeading = byId('home-heading');
    const currentUser = byId('current-user');
    const connectionStatus = byId('connection-status');
    const reconnectButton = byId('reconnect-button');
    const logoutButton = byId('logout-button');
    const tweetForm = byId('tweet-form');
    const tweetText = byId('tweet-text');
    const tweetCounter = byId('tweet-counter');
    const tweetButton = byId('tweet-button');
    const tweetMessage = byId('tweet-message');
    const followForm = byId('follow-form');
    const followUsername = byId('follow-username');
    const followMessage = byId('follow-message');
    const mentionSearchForm = byId('mention-search-form');
    const mentionSearchTerm = byId('mention-search-term');
    const hashtagSearchForm = byId('hashtag-search-form');
    const hashtagSearchTerm = byId('hashtag-search-term');
    const feedList = byId('feed-list');
    const feedEmpty = byId('feed-empty');
    const searchSummary = byId('search-summary');
    const searchList = byId('search-list');

    const pages = {
        login: { tab: byId('tab-login'), panel: byId('login-page') },
        register: { tab: byId('tab-register'), panel: byId('register-page') },
        home: { tab: byId('tab-home'), panel: byId('home-page') }
    };

    const session = { username: null, token: null, socket: null };
    const shownTweetIds = new Set();
    let newestTweetId = 0;
    let reconnectTimer = null;
    let reconnectAttempts = 0;
    // True while a tweet is on its way to the server
    let tweeting = false;

    // --- Pages and messages ----------------------------------------------------------------

    function showPage(name) {
        for (const [key, page] of Object.entries(pages)) {
            page.panel.hidden = key !== name;
            if (key === name) {
                page.tab.setAttribute('aria-current', 'page');
            } else {
                page.tab.removeAttribute('aria-current');
            }
        }
    }

    function setMessage(element, text, kind) {
        element.textContent = text || '';
        element.classList.toggle('error', kind === 'error');
        element.classList.toggle('success', kind === 'success');
    }

    function setStatus(state, text) {
        const defaultText = {
            connecting: 'Connecting...',
            live: 'Live',
            offline: 'Disconnected.'
        };
        connectionStatus.textContent = text || defaultText[state] || '';
        connectionStatus.dataset.state = state;
        reconnectButton.hidden = state !== 'offline';
    }

    // --- JSON API ---------------------------------------------------------------------------

    // Returns { ok, status, message, data }; never throws.
    async function api(method, path, body) {
        const headers = { Accept: 'application/json' };
        if (body !== undefined) {
            headers['Content-Type'] = 'application/json';
        }
        if (session.token) {
            headers.Authorization = 'Bearer ' + session.token;
        }

        let response;
        try {
            response = await fetch(path, {
                method: method,
                headers: headers,
                body: body === undefined ? undefined : JSON.stringify(body)
            });
        } catch (error) {
            return { ok: false, status: 0, message: 'Could not reach the server.', data: null };
        }

        let data = null;
        try {
            data = await response.json();
        } catch (error) {
            data = null;
        }

        const message = data && typeof data.message === 'string'
            ? data.message
            : 'The server sent an unexpected response (HTTP ' + response.status + ').';
        return { ok: response.ok && data !== null && data.ok === true, status: response.status, message: message, data: data };
    }

    // For the calls that need a session: a 401 means it has ended, so go back to the login page.
    async function apiAsUser(method, path, body) {
        const result = await api(method, path, body);
        if (result.status === 401 && session.token) {
            endSession('Your session has ended (the server may have restarted). Log in again.', 'error');
        }
        return result;
    }

    // Disables the form's buttons while the request runs.
    async function whileBusy(form, request) {
        const buttons = Array.from(form.querySelectorAll('button'));
        buttons.forEach((button) => { button.disabled = true; });
        try {
            return await request();
        } finally {
            buttons.forEach((button) => { button.disabled = false; });
            updateCounter();
        }
    }

    // --- Tweets in lists --------------------------------------------------------------------

    function isTweet(value) {
        return value !== null && typeof value === 'object' &&
            typeof value.id === 'number' && typeof value.author === 'string' &&
            typeof value.text === 'string' && typeof value.time === 'string';
    }

    function formatTime(iso) {
        const date = new Date(iso);
        if (Number.isNaN(date.getTime())) {
            return '';
        }
        return date.toLocaleString(undefined, { dateStyle: 'medium', timeStyle: 'short' });
    }

    function tweetItem(tweet) {
        const item = document.createElement('li');
        item.className = 'tweet';
        item.dataset.order = String(tweet.id);

        const meta = document.createElement('div');
        meta.className = 'tweet-meta';
        const author = document.createElement('span');
        author.className = 'tweet-author';
        author.textContent = '@' + tweet.author;
        const time = document.createElement('time');
        time.dateTime = tweet.time;
        time.textContent = formatTime(tweet.time);
        meta.append(author, time);

        const text = document.createElement('p');
        text.className = 'tweet-text';
        text.textContent = tweet.text;

        item.append(meta, text);
        return item;
    }

    // The feed is newest first. Tweets are ordered by id; a notice goes after the newest tweet
    // that was there when it arrived, and above any earlier notice in the same place.
    function insertIntoFeed(item) {
        const order = Number(item.dataset.order);
        const next = Array.from(feedList.children).find((child) => Number(child.dataset.order) <= order);
        feedList.insertBefore(item, next || null);

        while (feedList.children.length > MAX_FEED_ITEMS) {
            const last = feedList.lastElementChild;
            if (last.classList.contains('tweet')) {
                shownTweetIds.delete(Number(last.dataset.order));
            }
            last.remove();
        }
        feedEmpty.hidden = feedList.children.length > 0;
    }

    function addToFeed(tweet) {
        if (!isTweet(tweet) || shownTweetIds.has(tweet.id)) {
            return;
        }
        shownTweetIds.add(tweet.id);
        newestTweetId = Math.max(newestTweetId, tweet.id);
        insertIntoFeed(tweetItem(tweet));
    }

    function addNotice(text) {
        const item = document.createElement('li');
        item.className = 'notice';
        item.dataset.order = String(newestTweetId + 0.5);
        item.textContent = text;
        insertIntoFeed(item);
    }

    function clearFeed() {
        feedList.replaceChildren();
        shownTweetIds.clear();
        newestTweetId = 0;
        feedEmpty.hidden = false;
    }

    function showSearchResults(summary, tweets, kind) {
        searchList.replaceChildren(...tweets.filter(isTweet).map(tweetItem));
        setMessage(searchSummary, summary, kind);
    }

    function clearSearch() {
        showSearchResults('Search for a mention or a hashtag.', [], null);
    }

    // --- Live feed over a WebSocket -------------------------------------------------------------

    function socketUrl() {
        const url = new URL('ws', window.location.href);
        url.protocol = url.protocol === 'https:' ? 'wss:' : 'ws:';
        url.search = '';
        url.hash = '';
        return url.href;
    }

    function parseMessage(text) {
        try {
            const message = JSON.parse(text);
            return message !== null && typeof message === 'object' ? message : null;
        } catch (error) {
            return null;
        }
    }

    function handleSocketMessage(message) {
        if (message.type === 'feed') {
            // Added to what is shown (tweets appear once), so a reconnect keeps the follow notices
            (Array.isArray(message.tweets) ? message.tweets : []).forEach(addToFeed);
            reconnectAttempts = 0;
            setStatus('live');
        } else if (message.type === 'tweet') {
            addToFeed(message.tweet);
        } else if (message.type === 'follow' && typeof message.from === 'string') {
            addNotice('@' + message.from + ' followed you.');
        }
    }

    function connect() {
        disconnect();
        if (!session.token) {
            return;
        }
        setStatus('connecting');

        let socket;
        try {
            socket = new WebSocket(socketUrl());
        } catch (error) {
            scheduleReconnect();
            return;
        }
        session.socket = socket;
        // The server sends {"type": "error"} just before it closes the connection
        let problem = null;

        socket.addEventListener('open', () => {
            socket.send(JSON.stringify({ token: session.token }));
        });
        socket.addEventListener('message', (event) => {
            const message = socket === session.socket ? parseMessage(event.data) : null;
            if (message === null) {
                return;
            }
            if (message.type === 'error') {
                problem = typeof message.message === 'string' ? message.message : null;
            } else {
                handleSocketMessage(message);
            }
        });
        socket.addEventListener('close', (event) => {
            // Ignore sockets that were replaced or closed on purpose
            if (socket !== session.socket) {
                return;
            }
            session.socket = null;
            if (event.code === CLOSE_POLICY_VIOLATION) {
                // The session is no longer valid, for example because the server restarted
                endSession(problem || 'Your session has ended. Log in again.', 'error');
            } else {
                scheduleReconnect(problem);
            }
        });
    }

    // Tries again after a delay that grows with each failed try. The button tries at once.
    function scheduleReconnect(problem) {
        const delay = RECONNECT_DELAYS[Math.min(reconnectAttempts, RECONNECT_DELAYS.length - 1)];
        reconnectAttempts += 1;
        const wait = delay === 1 ? '1 second' : delay + ' seconds';
        setStatus('offline', (problem || 'Disconnected.') + ' Trying again in ' + wait + '.');
        clearTimeout(reconnectTimer);
        reconnectTimer = setTimeout(connect, delay * 1000);
    }

    function disconnect() {
        clearTimeout(reconnectTimer);
        reconnectTimer = null;
        const socket = session.socket;
        session.socket = null;
        if (socket) {
            socket.close(1000);
        }
    }

    // --- Session ------------------------------------------------------------------------------

    function startSession(username, token) {
        session.username = username;
        session.token = token;
        reconnectAttempts = 0;
        currentUser.textContent = '@' + username;
        clearFeed();
        clearSearch();
        [navMessage, tweetMessage, followMessage].forEach((element) => setMessage(element, ''));
        showPage('home');
        homeHeading.focus();
        connect();
    }

    function endSession(message, kind) {
        disconnect();
        session.username = null;
        session.token = null;
        currentUser.textContent = '';
        tweetForm.reset();
        followForm.reset();
        mentionSearchForm.reset();
        hashtagSearchForm.reset();
        updateCounter();
        clearFeed();
        clearSearch();
        setStatus('');
        [navMessage, tweetMessage, followMessage].forEach((element) => setMessage(element, ''));
        showPage('login');
        setMessage(loginMessage, message, kind);
        (loginUsername.value ? loginPassword : loginUsername).focus();
    }

    // --- Forms ----------------------------------------------------------------------------------

    function tweetLength() {
        return Array.from(tweetText.value.trim()).length;
    }

    function updateCounter() {
        const length = tweetLength();
        tweetCounter.textContent = length + ' / ' + MAX_TWEET_LENGTH;
        tweetCounter.classList.toggle('over', length > MAX_TWEET_LENGTH);
        // Stays disabled while a tweet is on its way, so that it is not sent twice
        tweetButton.disabled = tweeting || length === 0 || length > MAX_TWEET_LENGTH;
    }

    pages.login.tab.addEventListener('click', () => {
        if (session.token) {
            setMessage(navMessage, 'You are logged in as @' + session.username + '. Log out first to switch accounts.', 'error');
            return;
        }
        setMessage(navMessage, '');
        showPage('login');
    });

    pages.register.tab.addEventListener('click', () => {
        if (session.token) {
            setMessage(navMessage, 'You are logged in as @' + session.username + '. Log out first to register another account.', 'error');
            return;
        }
        setMessage(navMessage, '');
        showPage('register');
    });

    pages.home.tab.addEventListener('click', () => {
        if (!session.token) {
            setMessage(navMessage, 'Log in first to see your home page.', 'error');
            return;
        }
        setMessage(navMessage, '');
        showPage('home');
    });

    registerForm.addEventListener('submit', async (event) => {
        event.preventDefault();
        const username = registerUsername.value.trim();
        const password = registerPassword.value;
        if (!USERNAME_PATTERN.test(username)) {
            setMessage(registerMessage, USERNAME_RULE, 'error');
            return;
        }
        if (!password) {
            setMessage(registerMessage, 'Choose a password.', 'error');
            return;
        }
        // Counted in code points, as the server counts them (most emoji count once, not twice)
        if (Array.from(password).length > MAX_PASSWORD_LENGTH) {
            setMessage(registerMessage, PASSWORD_RULE, 'error');
            return;
        }

        const result = await whileBusy(registerForm, () => api('POST', 'api/register', { username: username, password: password }));
        if (!result.ok) {
            setMessage(registerMessage, result.message, 'error');
            return;
        }
        registerForm.reset();
        setMessage(registerMessage, '');
        loginUsername.value = username;
        showPage('login');
        setMessage(loginMessage, result.message, 'success');
        loginPassword.focus();
    });

    loginForm.addEventListener('submit', async (event) => {
        event.preventDefault();
        const username = loginUsername.value.trim();
        const password = loginPassword.value;
        if (!username || !password) {
            setMessage(loginMessage, 'Enter your username and password.', 'error');
            return;
        }

        const result = await whileBusy(loginForm, () => api('POST', 'api/login', { username: username, password: password }));
        loginPassword.value = '';
        if (!result.ok) {
            setMessage(loginMessage, result.message, 'error');
            return;
        }
        loginForm.reset();
        setMessage(loginMessage, '');
        startSession(result.data.username, result.data.token);
    });

    logoutButton.addEventListener('click', async () => {
        logoutButton.disabled = true;
        disconnect();
        const result = await api('POST', 'api/logout');
        logoutButton.disabled = false;
        // A 401 means that the session had already ended
        if (result.ok || result.status === 401) {
            endSession('You logged out.', 'success');
        } else {
            endSession('You are logged out on this page, but the server did not confirm that your session has ended.', 'error');
        }
    });

    reconnectButton.addEventListener('click', () => {
        // The button is hidden while connecting, so keep the focus close by
        homeHeading.focus();
        reconnectAttempts = 0;
        connect();
    });

    // The session token lives only in this page. When the page is closed or reloaded, end the
    // session on the server too; keepalive lets the request finish after the page has gone.
    window.addEventListener('pagehide', () => {
        if (!session.token) {
            return;
        }
        fetch('api/logout', { method: 'POST', keepalive: true, headers: { Authorization: 'Bearer ' + session.token } })
            .catch(() => {});
        endSession('You left the page, so you were logged out.', null);
    });

    tweetText.addEventListener('input', updateCounter);

    tweetForm.addEventListener('submit', async (event) => {
        event.preventDefault();
        if (tweeting) {
            return;
        }
        const text = tweetText.value.trim();
        const length = Array.from(text).length;
        if (length === 0 || length > MAX_TWEET_LENGTH) {
            setMessage(tweetMessage, 'Tweets are 1 to ' + MAX_TWEET_LENGTH + ' characters long.', 'error');
            return;
        }

        tweeting = true;
        updateCounter();
        let result;
        try {
            result = await apiAsUser('POST', 'api/tweet', { text: text });
        } finally {
            tweeting = false;
            updateCounter();
        }
        if (!result.ok) {
            setMessage(tweetMessage, result.message, 'error');
            return;
        }
        // Clear the box, unless more was typed while the tweet was on its way
        if (tweetText.value.trim() === text) {
            tweetForm.reset();
            updateCounter();
        }
        setMessage(tweetMessage, result.message, 'success');
        // The tweet also arrives over the WebSocket; addToFeed shows it once
        addToFeed(result.data.tweet);
    });

    followForm.addEventListener('submit', async (event) => {
        event.preventDefault();
        const action = event.submitter && event.submitter.value === 'unfollow' ? 'unfollow' : 'follow';
        const username = followUsername.value.trim().replace(/^@+/, '');
        if (!username) {
            setMessage(followMessage, 'Type a username to ' + action + '.', 'error');
            return;
        }

        const result = await whileBusy(followForm, () => apiAsUser('POST', 'api/' + action, { username: username }));
        setMessage(followMessage, result.message, result.ok ? 'success' : 'error');
        if (result.ok) {
            followForm.reset();
        }
    });

    async function search(form, kind, input, prefix) {
        const term = input.value.trim().replace(prefix, '').trim();
        if (!term) {
            showSearchResults('Type something to search for.', [], 'error');
            return;
        }

        const result = await whileBusy(form, () => api('GET', 'api/search/' + kind + '/' + encodeURIComponent(term)));
        if (!result.ok) {
            showSearchResults(result.message, [], 'error');
            return;
        }
        showSearchResults(result.message, Array.isArray(result.data.tweets) ? result.data.tweets : [], null);
    }

    mentionSearchForm.addEventListener('submit', (event) => {
        event.preventDefault();
        search(mentionSearchForm, 'mentions', mentionSearchTerm, /^@+/);
    });

    hashtagSearchForm.addEventListener('submit', (event) => {
        event.preventDefault();
        search(hashtagSearchForm, 'hashtags', hashtagSearchTerm, /^#+/);
    });

    showPage('login');
    setStatus('');
    updateCounter();
})();
