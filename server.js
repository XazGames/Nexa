const WebSocket = require('ws');
const { v4: uuidv4 } = require('uuid');

const wss = new WebSocket.Server({ port: 8080 });

// Хранилище (в памяти – для демо, замени на БД)
const users = {};                 // token -> { id, name, channels }
const channels = {};             // channelId -> { id, name, isPrivate, members: [userId], messages: [{ id, userId, userName, text, timestamp }] }

// Начальный канал
const mainChannelId = uuidv4();
channels[mainChannelId] = {
  id: mainChannelId,
  name: 'Главный',
  isPrivate: false,
  members: [],
  messages: [],
};

// Утилиты
function broadcast(channelId, data) {
  wss.clients.forEach(client => {
    if (client.readyState === WebSocket.OPEN && client.channelId === channelId) {
      client.send(JSON.stringify(data));
    }
  });
}

wss.on('connection', (ws) => {
  ws.token = null;
  ws.userId = null;
  ws.channelId = mainChannelId; // по умолчанию

  ws.on('message', (raw) => {
    let msg;
    try { msg = JSON.parse(raw); } catch (e) { return; }

    const { type, payload } = msg;

    // --- АВТОРИЗАЦИЯ ---
    if (type === 'auth') {
      const { action, name, password } = payload;
      // В демо пароли не хешируются, для продакшена используй bcrypt
      if (action === 'register') {
        if (users[name]) {
          ws.send(JSON.stringify({ type: 'auth_result', payload: { success: false, error: 'Пользователь уже существует' } }));
          return;
        }
        const userId = uuidv4();
        const token = uuidv4();
        users[name] = { id: userId, name, password, token };
        ws.token = token;
        ws.userId = userId;
        ws.send(JSON.stringify({ type: 'auth_result', payload: { success: true, token, user: { id: userId, name } } }));
        // Отправить список каналов
        ws.send(JSON.stringify({ type: 'channels_list', payload: Object.values(channels).map(c => ({ id: c.id, name: c.name, isPrivate: c.isPrivate })) }));
        return;
      }
      if (action === 'login') {
        const user = users[name];
        if (!user || user.password !== password) {
          ws.send(JSON.stringify({ type: 'auth_result', payload: { success: false, error: 'Неверные логин или пароль' } }));
          return;
        }
        ws.token = user.token;
        ws.userId = user.id;
        ws.send(JSON.stringify({ type: 'auth_result', payload: { success: true, token: user.token, user: { id: user.id, name } } }));
        ws.send(JSON.stringify({ type: 'channels_list', payload: Object.values(channels).map(c => ({ id: c.id, name: c.name, isPrivate: c.isPrivate })) }));
        return;
      }
    }

    // Проверка авторизации для остальных действий
    if (!ws.token || !users[Object.keys(users).find(u => users[u].token === ws.token)]) {
      ws.send(JSON.stringify({ type: 'error', payload: 'Не авторизован' }));
      return;
    }

    const currentUser = Object.values(users).find(u => u.token === ws.token);

    // --- СОЗДАНИЕ КАНАЛА ---
    if (type === 'create_channel') {
      const { name, isPrivate } = payload;
      const id = uuidv4();
      channels[id] = {
        id,
        name,
        isPrivate,
        members: [currentUser.id],
        messages: [],
      };
      // Уведомить всех клиентов о новом канале
      wss.clients.forEach(client => {
        if (client.readyState === WebSocket.OPEN) {
          client.send(JSON.stringify({ type: 'channels_list', payload: Object.values(channels).map(c => ({ id: c.id, name: c.name, isPrivate: c.isPrivate })) }));
        }
      });
      ws.send(JSON.stringify({ type: 'channel_created', payload: { id, name, isPrivate } }));
    }

    // --- ПРИСОЕДИНИТЬСЯ К КАНАЛУ ---
    if (type === 'join_channel') {
      const { channelId } = payload;
      if (!channels[channelId]) {
        ws.send(JSON.stringify({ type: 'error', payload: 'Канал не найден' }));
        return;
      }
      const channel = channels[channelId];
      if (channel.isPrivate && !channel.members.includes(currentUser.id)) {
        // Для приватных каналов просто запрещаем (можно добавить приглашения)
        ws.send(JSON.stringify({ type: 'error', payload: 'Доступ запрещён' }));
        return;
      }
      ws.channelId = channelId;
      if (!channel.members.includes(currentUser.id)) {
        channel.members.push(currentUser.id);
      }
      // Отправить историю сообщений канала
      ws.send(JSON.stringify({ type: 'channel_history', payload: { channelId, messages: channel.messages } }));
    }

    // --- ОТПРАВКА СООБЩЕНИЯ ---
    if (type === 'send_message') {
      const { text } = payload;
      const channel = channels[ws.channelId];
      if (!channel) return;
      const message = {
        id: uuidv4(),
        userId: currentUser.id,
        userName: currentUser.name,
        text,
        timestamp: Date.now(),
      };
      channel.messages.push(message);
      // Отправить всем в этом канале
      broadcast(ws.channelId, { type: 'new_message', payload: message });
    }

    // --- ПОЛУЧИТЬ СПИСОК КАНАЛОВ ---
    if (type === 'get_channels') {
      ws.send(JSON.stringify({ type: 'channels_list', payload: Object.values(channels).map(c => ({ id: c.id, name: c.name, isPrivate: c.isPrivate })) }));
    }
  });

  ws.on('close', () => {
    // Ничего не делаем
  });
});

console.log('Nexa server running on ws://localhost:8080');