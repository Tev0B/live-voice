(() => {
  const status = document.querySelector('#status');
  const list = document.querySelector('#session-list');
  const backButton = document.querySelector('#back');
  const enableSoundButton = document.querySelector('#enable-sound');
  const audio = document.querySelector('#audio');
  const config = window.VOICE_DEMO_CONFIG || {};
  const iceServers = config.ICE_SERVERS || [{ urls: 'stun:stun.l.google.com:19302' }];

  let socket = null;
  let pc = null;
  let pendingCandidates = [];
  let connected = false;
  let audioBlocked = false;
  let joining = false;
  let selectedSessionId = null;
  let generation = 0;
  let connectTimer = null;
  let negotiationTimer = null;
  let reconnectTimer = null;
  let retryDelay = 1000;
  let intentionalClose = false;
  let reconnectNeeded = true;
  let latestSessions = [];

  function setStatus(text, state = '') { status.textContent = text; status.dataset.state = state; }
  function signalingUrl() {
    return config.SIGNAL_URL || `${location.protocol === 'https:' ? 'wss:' : 'ws:'}//${location.host}/signal`;
  }
  function send(message) {
    if (socket && socket.readyState === WebSocket.OPEN) socket.send(JSON.stringify(message));
  }
  function clearTimers() {
    clearTimeout(connectTimer); clearTimeout(negotiationTimer); clearTimeout(reconnectTimer);
    connectTimer = negotiationTimer = reconnectTimer = null;
  }
  function closePeer(preservePendingCandidates = false) {
    clearTimeout(negotiationTimer); negotiationTimer = null;
    if (pc) { pc.onicecandidate = null; pc.ontrack = null; pc.onconnectionstatechange = null; try { pc.close(); } catch (_) {} }
    pc = null;
    if (!preservePendingCandidates) pendingCandidates = [];
    connected = false; audio.srcObject = null;
  }
  async function drainCandidates(peer, myGeneration) {
    if (!peer.remoteDescription) return;
    while (pendingCandidates.length && peer === pc && myGeneration === generation) {
      const candidate = pendingCandidates.shift();
      try { await peer.addIceCandidate(candidate); } catch (err) { console.warn('Could not add ICE candidate:', err.message); }
    }
  }
  function renderSessions(sessions) {
    latestSessions = Array.isArray(sessions) ? sessions : [];
    if (selectedSessionId) return;
    list.replaceChildren();
    if (!latestSessions.length) {
      const empty = document.createElement('div');
      empty.className = 'empty'; empty.textContent = 'Активных сессий пока нет';
      list.appendChild(empty); return;
    }
    for (const session of latestSessions) {
      const item = document.createElement('button');
      item.className = 'session-item';
      const name = document.createElement('span'); name.className = 'session-name';
      name.textContent = session.name || `Сессия ${new Date(session.createdAt).toLocaleTimeString('ru-RU')}`;
      const meta = document.createElement('span'); meta.className = 'session-meta';
      const count = Number(session.listenerCount) || 0;
      meta.textContent = count ? `${count} слуш.` : 'Свободна';
      item.append(name, meta);
      item.addEventListener('click', () => selectSession(session.id));
      list.appendChild(item);
    }
  }
  function showSessionList() {
    selectedSessionId = null;
    joining = false;
    backButton.hidden = true;
    list.hidden = false;
    renderSessions(latestSessions);
  }

  async function handleOffer(offer, myGeneration) {
    if (myGeneration !== generation || !selectedSessionId) return;
    closePeer(true);
    const peer = new RTCPeerConnection({ iceServers, iceCandidatePoolSize: 2 });
    pc = peer;
    let answerSent = false;
    const localCandidateQueue = [];
    peer.onicecandidate = event => {
      if (!event.candidate || pc !== peer || socket?.readyState !== WebSocket.OPEN) return;
      const candidate = { type: 'signal', signal: { kind: 'candidate', data: event.candidate.toJSON() } };
      if (answerSent) send(candidate); else localCandidateQueue.push(candidate);
    };
    peer.ontrack = async event => {
      if (pc !== peer || myGeneration !== generation) return;
      if (event.streams?.[0]) audio.srcObject = event.streams[0];
      else {
        let remoteStream = audio.srcObject;
        if (!remoteStream) { remoteStream = new MediaStream(); audio.srcObject = remoteStream; }
        if (!remoteStream.getTracks().some(track => track.id === event.track.id)) remoteStream.addTrack(event.track);
      }
      audio.muted = false; audio.volume = 1;
      try {
        await audio.play(); audioBlocked = false; enableSoundButton.hidden = true;
        if (connected) setStatus('В эфире', 'active');
      } catch (_) {
        audioBlocked = true; enableSoundButton.hidden = false;
        setStatus('Нажмите «Включить звук»', 'waiting');
      }
    };
    peer.onconnectionstatechange = () => {
      if (pc !== peer || myGeneration !== generation) return;
      if (peer.connectionState === 'connected') {
        connected = true; clearTimeout(negotiationTimer); negotiationTimer = null;
        setStatus(audioBlocked ? 'Нажмите «Включить звук»' : 'В эфире', audioBlocked ? 'waiting' : 'active');
      } else if (peer.connectionState === 'failed') {
        connected = false; clearTimeout(negotiationTimer); negotiationTimer = null;
        setStatus('Не удалось соединиться. В этой сети может требоваться TURN.', 'error');
        backButton.hidden = false;
      } else if (peer.connectionState === 'disconnected') {
        setStatus('Связь прервана', 'waiting');
      }
    };

    await peer.setRemoteDescription(offer);
    await drainCandidates(peer, myGeneration);
    const answer = await peer.createAnswer();
    await peer.setLocalDescription(answer);
    if (pc !== peer || myGeneration !== generation) return;
    send({ type: 'signal', signal: { kind: 'answer', data: peer.localDescription } });
    answerSent = true;
    while (localCandidateQueue.length) send(localCandidateQueue.shift());
    setStatus('Подключаем звук…', 'waiting');
    clearTimeout(negotiationTimer);
    negotiationTimer = setTimeout(() => {
      if (pc === peer && peer.connectionState !== 'connected') {
        setStatus('Подключение заняло слишком много времени. Может потребоваться TURN.', 'error');
        backButton.hidden = false;
      }
    }, 12000);
  }

  function disconnectFromSession(sendLeave = true) {
    clearTimeout(connectTimer); connectTimer = null;
    selectedSessionId = null; joining = false; connected = false;
    closePeer();
    if (sendLeave && socket?.readyState === WebSocket.OPEN) send({ type: 'leave' });
    showSessionList();
  }

  function selectSession(sessionId) {
    if (!socket || socket.readyState !== WebSocket.OPEN) {
      setStatus('Нет соединения с сервером. Переподключаемся…', 'error'); return;
    }
    const session = latestSessions.find(s => s.id === sessionId);
    if (!session) { send({ type: 'list-sessions' }); setStatus('Сессия уже завершена. Выберите другую.', 'error'); return; }
    selectedSessionId = sessionId; joining = true; connected = false; audioBlocked = false;
    enableSoundButton.hidden = true; list.hidden = true; backButton.hidden = false;
    setStatus('Подключаемся к сессии…', 'waiting');
    const myGeneration = ++generation;
    send({ type: 'join-session', sessionId });
    clearTimeout(connectTimer);
    connectTimer = setTimeout(() => {
      if (selectedSessionId === sessionId && joining && generation === myGeneration) {
        setStatus('Сервер не ответил. Вернитесь к списку и попробуйте ещё раз.', 'error');
        backButton.hidden = false;
      }
    }, 6500);
  }

  function connectSignaling() {
    if (intentionalClose) return;
    setStatus('Подключаемся к серверу…', 'waiting');
    let ws;
    try { ws = new WebSocket(signalingUrl()); }
    catch (_) { scheduleReconnect(); return; }
    socket = ws;
    ws.addEventListener('open', () => {
      if (socket !== ws) return;
      retryDelay = 1000;
      send({ type: 'list-sessions' });
      setStatus(selectedSessionId ? 'Переподключение…' : 'Выберите сессию', selectedSessionId ? 'waiting' : '');
      // A page reconnect only refreshes the list; it never silently rejoins an audio session.
      if (selectedSessionId) {
        selectedSessionId = null; joining = false; closePeer(); showSessionList();
      }
    });
    ws.addEventListener('message', event => {
      if (socket !== ws) return;
      let msg; try { msg = JSON.parse(event.data); } catch (_) { return; }
      if (msg.type === 'sessions') {
        renderSessions(msg.sessions);
        if (!selectedSessionId && ws.readyState === WebSocket.OPEN) {
          setStatus(msg.sessions?.length ? 'Выберите сессию' : 'Активных сессий пока нет', '');
        }
      } else if (msg.type === 'joined-session') {
        clearTimeout(connectTimer); connectTimer = null; joining = true;
        setStatus('Подключаем звук…', 'waiting');
        clearTimeout(negotiationTimer);
        const joinedId = selectedSessionId;
        negotiationTimer = setTimeout(() => {
          if (selectedSessionId === joinedId && !connected) {
            setStatus('Не удалось начать аудиосвязь. Попробуйте ещё раз; иногда нужен TURN.', 'error');
            backButton.hidden = false;
          }
        }, 12000);
      } else if (msg.type === 'signal' && msg.signal) {
        const myGeneration = generation;
        if (msg.signal.kind === 'offer') {
          handleOffer(msg.signal.data, myGeneration).catch(err => {
            console.error(err); setStatus('Ошибка аудиосоединения', 'error'); backButton.hidden = false;
          });
        } else if (msg.signal.kind === 'candidate' && msg.signal.data) {
          const candidate = msg.signal.data;
          if (pc?.remoteDescription) pc.addIceCandidate(candidate).catch(err => console.warn(err.message));
          else pendingCandidates.push(candidate);
        }
      } else if (msg.type === 'join-failed') {
        clearTimeout(connectTimer); connectTimer = null;
        selectedSessionId = null; joining = false; closePeer(); showSessionList();
        setStatus(msg.message || 'Не удалось подключиться к сессии', 'error');
        send({ type: 'list-sessions' });
      } else if (msg.type === 'session-ended') {
        clearTimeout(connectTimer); selectedSessionId = null; joining = false; closePeer();
        showSessionList(); setStatus('Сессия завершена', 'error');
        send({ type: 'list-sessions' });
      } else if (msg.type === 'error') {
        setStatus(msg.message || 'Ошибка сервера', 'error');
      }
    });
    ws.addEventListener('close', () => {
      if (socket !== ws || intentionalClose) return;
      socket = null; closePeer(); selectedSessionId = null; joining = false;
      showSessionList(); setStatus('Соединение потеряно. Переподключаемся…', 'error');
      scheduleReconnect();
    });
    ws.addEventListener('error', () => {
      if (socket === ws) setStatus('Не удалось подключиться к серверу', 'error');
    });
  }

  function scheduleReconnect() {
    if (intentionalClose || reconnectTimer) return;
    reconnectTimer = setTimeout(() => {
      reconnectTimer = null;
      if (!intentionalClose) connectSignaling();
    }, retryDelay);
    retryDelay = Math.min(retryDelay * 2, 10000);
  }

  enableSoundButton.addEventListener('click', async () => {
    try { await audio.play(); audioBlocked = false; enableSoundButton.hidden = true; if (connected) setStatus('В эфире', 'active'); }
    catch (_) { setStatus('Не удалось включить звук. Проверьте громкость браузера.', 'error'); }
  });

  backButton.addEventListener('click', () => {
    disconnectFromSession(true);
    setStatus(socket?.readyState === WebSocket.OPEN ? 'Выберите сессию' : 'Подключаемся к серверу…');
    if (socket?.readyState === WebSocket.OPEN) send({ type: 'list-sessions' });
  });
  window.addEventListener('pagehide', () => {
    intentionalClose = true; clearTimers();
    if (socket?.readyState === WebSocket.OPEN) { try { send({ type: 'leave' }); } catch (_) {} }
    if (socket) { try { socket.close(); } catch (_) {} }
    closePeer();
  });

  connectSignaling();
})();
