(() => {
  const button = document.querySelector('#toggle');
  const status = document.querySelector('#status');
  const config = window.VOICE_DEMO_CONFIG || {};
  const iceServers = config.ICE_SERVERS || [{ urls: 'stun:stun.l.google.com:19302' }];
  let socket = null;
  let stream = null;
  let active = false;
  let starting = false;
  let sessionId = null;
  let generation = 0;
  let timers = [];
  const peers = new Map();

  function setStatus(text, state = '') { status.textContent = text; status.dataset.state = state; }
  function signalingUrl() {
    return config.SIGNAL_URL || `${location.protocol === 'https:' ? 'wss:' : 'ws:'}//${location.host}/signal`;
  }
  function later(fn, ms) { const id = setTimeout(fn, ms); timers.push(id); return id; }
  function clearTimers() { timers.forEach(clearTimeout); timers = []; }
  function send(message) {
    if (socket && socket.readyState === WebSocket.OPEN) socket.send(JSON.stringify(message));
  }
  function waitForOpen(ws, timeoutMs = 6500) {
    return new Promise((resolve, reject) => {
      let done = false;
      const finish = error => {
        if (done) return;
        done = true; clearTimeout(timer);
        ws.removeEventListener('open', onOpen);
        ws.removeEventListener('error', onError);
        ws.removeEventListener('close', onClose);
        error ? reject(error) : resolve();
      };
      const onOpen = () => finish();
      const onError = () => finish(new Error('Не удалось подключиться к серверу.'));
      const onClose = () => finish(new Error('Сервер закрыл соединение.'));
      const timer = setTimeout(() => finish(new Error('Сервер не ответил за 6,5 секунды. Проверьте адрес и HTTPS/WSS.')), timeoutMs);
      ws.addEventListener('open', onOpen); ws.addEventListener('error', onError); ws.addEventListener('close', onClose);
    });
  }
  function closePeer(listenerId) {
    const entry = peers.get(listenerId);
    if (!entry) return;
    entry.pc.onicecandidate = null;
    entry.pc.onconnectionstatechange = null;
    try { entry.pc.close(); } catch (_) {}
    peers.delete(listenerId);
  }
  function closeAllPeers() { for (const id of [...peers.keys()]) closePeer(id); }

  async function addCandidates(entry) {
    if (!entry.pc.remoteDescription) return;
    while (entry.pendingCandidates.length) {
      const candidate = entry.pendingCandidates.shift();
      try { await entry.pc.addIceCandidate(candidate); } catch (err) { console.warn('Could not add ICE candidate:', err.message); }
    }
  }

  async function createOfferForListener(listenerId, myGeneration) {
    if (!active || !stream || !socket || socket.readyState !== WebSocket.OPEN || myGeneration !== generation) return;
    closePeer(listenerId);
    const pc = new RTCPeerConnection({ iceServers, iceCandidatePoolSize: 2 });
    const entry = { pc, pendingCandidates: [], offerSent: false, localCandidateQueue: [] };
    peers.set(listenerId, entry);
    stream.getAudioTracks().forEach(track => pc.addTrack(track, stream));

    pc.onicecandidate = event => {
      if (!event.candidate || peers.get(listenerId) !== entry) return;
      const candidate = { type: 'signal', targetId: listenerId, signal: { kind: 'candidate', data: event.candidate.toJSON() } };
      if (entry.offerSent) send(candidate); else entry.localCandidateQueue.push(candidate);
    };
    pc.onconnectionstatechange = () => {
      if (peers.get(listenerId) !== entry || myGeneration !== generation) return;
      if (pc.connectionState === 'failed') {
        console.warn('WebRTC failed for listener', listenerId, 'A TURN server may be required.');
        setStatus('Сессия начата · проверьте сеть слушателя', 'waiting');
      } else if (pc.connectionState === 'connected') {
        setStatus('Сессия начата', 'active');
      }
    };

    const offer = await pc.createOffer();
    await pc.setLocalDescription(offer);
    // Trickle ICE: send the offer immediately, then relay ICE candidates as they appear.
    // This avoids waiting several seconds for ICE gathering to finish.
    if (myGeneration === generation && peers.get(listenerId) === entry) {
      send({ type: 'signal', targetId: listenerId, signal: { kind: 'offer', data: pc.localDescription } });
      entry.offerSent = true;
      while (entry.localCandidateQueue.length) send(entry.localCandidateQueue.shift());
    }
  }

  async function handleMessage(ws, msg, myGeneration) {
    if (socket !== ws || generation !== myGeneration) return;
    if (msg.type === 'session-created') {
      clearTimers(); sessionId = msg.session.id; active = true; starting = false;
      button.disabled = false; button.classList.add('stop'); button.textContent = 'Завершить сессию';
      setStatus('Сессия начата', 'active');
    } else if (msg.type === 'listener-joined') {
      try { await createOfferForListener(msg.listenerId, myGeneration); }
      catch (err) { console.error(err); closePeer(msg.listenerId); }
    } else if (msg.type === 'listener-left') {
      closePeer(msg.listenerId);
    } else if (msg.type === 'signal' && msg.signal && msg.from) {
      const entry = peers.get(msg.from);
      if (!entry) return;
      try {
        if (msg.signal.kind === 'answer') {
          await entry.pc.setRemoteDescription(msg.signal.data);
          await addCandidates(entry);
        } else if (msg.signal.kind === 'candidate' && msg.signal.data) {
          if (entry.pc.remoteDescription) await entry.pc.addIceCandidate(msg.signal.data);
          else entry.pendingCandidates.push(msg.signal.data);
        }
      } catch (err) { console.warn('WebRTC signaling error:', err.message); }
    } else if (msg.type === 'error') {
      const message = msg.message || 'Ошибка сервера.';
      cleanup(false);
      setStatus(message, 'error');
    }
  }

  function cleanup(sendLeave = true) {
    generation++;
    clearTimers();
    const oldSocket = socket;
    socket = null;
    if (sendLeave && oldSocket && oldSocket.readyState === WebSocket.OPEN) {
      try { oldSocket.send(JSON.stringify({ type: 'leave' })); } catch (_) {}
    }
    if (oldSocket) { try { oldSocket.close(1000, 'Session ended'); } catch (_) {} }
    closeAllPeers();
    if (stream) stream.getTracks().forEach(track => track.stop());
    stream = null; active = false; starting = false; sessionId = null;
    button.disabled = false; button.classList.remove('stop'); button.textContent = 'Начать сессию';
  }

  async function startSession() {
    if (active || starting) {
      cleanup(true); setStatus('Сессия завершена'); return;
    }
    // Browser permission is not a substitute for letting the speaker know who may listen.
    const confirmed = window.confirm('После начала сессия появится в общем списке, и посетители страницы слушателя смогут слышать микрофон. Продолжить?');
    if (!confirmed) return;

    starting = true;
    const myGeneration = ++generation;
    button.disabled = true;
    setStatus('Запрашиваем микрофон…', 'waiting');
    try {
      if (!navigator.mediaDevices?.getUserMedia) throw new Error('Для микрофона откройте страницу через localhost или HTTPS.');
      stream = await navigator.mediaDevices.getUserMedia({
        audio: { echoCancellation: true, noiseSuppression: true, autoGainControl: true }, video: false
      });
      if (myGeneration !== generation) { stream.getTracks().forEach(t => t.stop()); return; }

      const ws = new WebSocket(signalingUrl());
      socket = ws;
      ws.addEventListener('message', event => {
        let msg; try { msg = JSON.parse(event.data); } catch (_) { return; }
        handleMessage(ws, msg, myGeneration).catch(err => console.error(err));
      });
      ws.addEventListener('close', () => {
        if (socket !== ws || myGeneration !== generation) return;
        cleanup(false); setStatus('Соединение с сервером потеряно', 'error');
      });
      await waitForOpen(ws);
      if (socket !== ws || myGeneration !== generation) return;
      const name = `Сессия · ${new Date().toLocaleTimeString('ru-RU')}`;
      send({ type: 'create-session', name });
      later(() => {
        if (socket === ws && starting) {
          cleanup(false); setStatus('Сервер не ответил. Попробуйте ещё раз.', 'error');
        }
      }, 6500);
    } catch (err) {
      if (myGeneration === generation) {
        cleanup(false);
        setStatus(err.message || 'Не удалось начать сессию', 'error');
      }
    }
  }

  button.addEventListener('click', startSession);
  window.addEventListener('pagehide', () => cleanup(true));
})();
