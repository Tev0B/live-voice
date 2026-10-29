/*
 * Local test: leave SIGNAL_URL null.
 * For separate websites, set this to the same public signaling endpoint on both pages,
 * such as "wss://voice.example.com/signal". HTTPS pages must use WSS.
 * STUN usually suffices, but a TURN server may be needed in restrictive networks.
 */
window.VOICE_DEMO_CONFIG = {
  SIGNAL_URL: null,
  ICE_SERVERS: [
    { urls: 'stun:stun.l.google.com:19302' },
    { urls: 'stun:stun.cloudflare.com:3478' }
    // Optional TURN example (use your own service):
    // { urls: 'turn:turn.example.com:3478', username: 'USER', credential: 'PASSWORD' }
  ]
};
