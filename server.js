const WebSocket = require('ws');
const { v4: uuidv4 } = require('uuid');
const bcrypt = require('bcrypt');
const sqlite3 = require('sqlite3').verbose();
const path = require('path');

// --- БД ---
const db = new sqlite3.Database(path.join(__dirname, 'database.sqlite'));

db.serialize(() => {
  db.run(`
    CREATE TABLE IF NOT EXISTS users (
      id TEXT PRIMARY KEY,
      name TEXT UNIQUE,
      password TEXT,
      token TEXT
    )
  `);
  db.run(`
    CREATE TABLE IF NOT EXISTS channels (
      id TEXT PRIMARY KEY,
      name TEXT,
      isPrivate INTEGER,
      ownerId TEXT
    )
  `);
  db.run(`
    CREATE TABLE IF NOT EXISTS channel_members (
      channelId TEXT,
      userId TEXT,
      PRIMARY KEY (channelId, userId)
    )
  `);
  db.run(`
    CREATE TABLE IF NOT EXISTS messages (
      id TEXT PRIMARY KEY,
      channelId TEXT,
      userId TEXT,
      userName TEXT,
      text TEXT,
      timestamp INTEGER
    )
  `);
  // Создаём главный канал, если его нет
  db.get("SELECT id FROM channels WHERE name = 'Главный'", (err, row) => {
    if (!row) {
      const mainId = uuidv4();
      db.run("INSERT INTO channels (id, name, isPrivate, ownerId) VALUES (?, ?, ?, ?)", [mainId, 'Главный', 0, null]);
    }
  });
});

// --- WebSocket сервер ---
const wss = new WebSocket.Server({ port: 8080 });

// Храним клиентов: ws -> { userId, channelId, typingTimeout }
const clients = new Map();

// --- Вспомогательные функции для БД (промисы) ---
function dbGet(sql, params = []) {
  return new Promise((resolve, reject) => {
    db.get(sql, params, (err, row) => { if (err) reject(err); else resolve(row); });
  });
}
function dbAll(sql, params = []) {
  return new Promise((resolve, reject) => {
    db.all(sql, params, (err, rows) => { if (err) reject(err); else resolve(rows); });
  });
}
function dbRun(sql, params = []) {
  return new Promise((resolve, reject) => {
    db.run(sql, params, function(err) { if (err) reject(err); else resolve(this); });
  });
}

// --- Отправка сообщения клиенту ---
function sendTo(ws, data) {
  if (ws.readyState === WebSocket.OPEN) {
    ws.send(JSON.stringify(data));
  }
}

// --- Рассылка в канал (всем участникам) ---
async function broadcastToChannel(channelId, data, excludeWs = null) {
  const members = await dbAll("SELECT userId FROM channel_members WHERE channelId = ?", [channelId]);
  const userIds = members.map(m => m.userId);
  for (const [ws, info] of clients) {
    if (ws === excludeWs) continue;
    if (info && userIds.includes(info.userId)) {
      sendTo(ws, data);
    }
  }
}

// --- Обработка сообщений от клиента ---
wss.on('connection', (ws) => {
  const clientInfo = { userId: null, channelId: null, typingTimeout: null };
  clients.set(ws, clientInfo);

  ws.on('message', async (raw) => {
    let msg;
    try { msg = JSON.parse(raw); } catch (e) { return; }
    const { type, payload } = msg;

    // --- АВТОРИЗАЦИЯ ---
    if (type === 'auth') {
      const { action, name, password } = payload;
      try {
        if (action === 'register') {
          const existing = await dbGet("SELECT id FROM users WHERE name = ?", [name]);
          if (existing) {
            sendTo(ws, { type: 'auth_result', payload: { success: false, error: 'Имя уже занято' } });
            return;
          }
          const id = uuidv4();
          const token = uuidv4();
          const hash = await bcrypt.hash(password, 10);
          await dbRun("INSERT INTO users (id, name, password, token) VALUES (?, ?, ?, ?)", [id, name, hash, token]);
          // Добавляем в главный канал
          const main = await dbGet("SELECT id FROM channels WHERE name = 'Главный'");
          if (main) {
            await dbRun("INSERT OR IGNORE INTO channel_members (channelId, userId) VALUES (?, ?)", [main.id, id]);
          }
          clientInfo.userId = id;
          sendTo(ws, { type: 'auth_result', payload: { success: true, token, user: { id, name } } });
          sendChannelsList(ws);
          sendChannelHistory(ws, main ? main.id : null);
          return;
        }
        if (action === 'login') {
          const user = await dbGet("SELECT * FROM users WHERE name = ?", [name]);
          if (!user) {
            sendTo(ws, { type: 'auth_result', payload: { success: false, error: 'Неверные данные' } });
            return;
          }
          const match = await bcrypt.compare(password, user.password);
          if (!match) {
            sendTo(ws, { type: 'auth_result', payload: { success: false, error: 'Неверные данные' } });
            return;
          }
          const token = uuidv4();
          await dbRun("UPDATE users SET token = ? WHERE id = ?", [token, user.id]);
          clientInfo.userId = user.id;
          sendTo(ws, { type: 'auth_result', payload: { success: true, token, user: { id: user.id, name: user.name } } });
          sendChannelsList(ws);
          // Отправить текущий канал (последний использованный или главный)
          const main = await dbGet("SELECT id FROM channels WHERE name = 'Главный'");
          if (main) {
            clientInfo.channelId = main.id;
            sendChannelHistory(ws, main.id);
          }
          return;
        }
      } catch (err) {
        sendTo(ws, { type: 'auth_result', payload: { success: false, error: 'Ошибка сервера' } });
        console.error(err);
      }
      return;
    }

    // --- Проверка авторизации для остальных команд ---
    if (!clientInfo.userId) {
      sendTo(ws, { type: 'error', payload: 'Не авторизован' });
      return;
    }

    // --- ПОЛУЧИТЬ СПИСОК КАНАЛОВ ---
    if (type === 'get_channels') {
      sendChannelsList(ws);
      return;
    }

    // --- ПРИСОЕДИНИТЬСЯ К КАНАЛУ ---
    if (type === 'join_channel') {
      const { channelId } = payload;
      // Проверяем, есть ли канал и является ли участником
      const channel = await dbGet("SELECT * FROM channels WHERE id = ?", [channelId]);
      if (!channel) {
        sendTo(ws, { type: 'error', payload: 'Канал не найден' });
        return;
      }
      const isMember = await dbGet("SELECT * FROM channel_members WHERE channelId = ? AND userId = ?", [channelId, clientInfo.userId]);
      if (channel.isPrivate && !isMember) {
        sendTo(ws, { type: 'error', payload: 'Доступ запрещён' });
        return;
      }
      // Добавляем в участники, если ещё нет
      if (!isMember) {
        await dbRun("INSERT INTO channel_members (channelId, userId) VALUES (?, ?)", [channelId, clientInfo.userId]);
      }
      clientInfo.channelId = channelId;
      sendChannelHistory(ws, channelId);
      return;
    }

    // --- ОТПРАВКА СООБЩЕНИЯ ---
    if (type === 'send_message') {
      const { text } = payload;
      const channelId = clientInfo.channelId;
      if (!channelId) return;
      const user = await dbGet("SELECT name FROM users WHERE id = ?", [clientInfo.userId]);
      const messageId = uuidv4();
      const timestamp = Date.now();
      await dbRun(
        "INSERT INTO messages (id, channelId, userId, userName, text, timestamp) VALUES (?, ?, ?, ?, ?, ?)",
        [messageId, channelId, clientInfo.userId, user.name, text, timestamp]
      );
      const messageData = { id: messageId, userId: clientInfo.userId, userName: user.name, text, timestamp };
      // Рассылаем всем в канале
      broadcastToChannel(channelId, { type: 'new_message', payload: messageData }, ws);
      // Отправляем подтверждение отправителю (чтобы он сразу увидел)
      sendTo(ws, { type: 'new_message', payload: messageData });
      return;
    }

    // --- СОЗДАНИЕ КАНАЛА ---
    if (type === 'create_channel') {
      const { name, isPrivate } = payload;
      if (!name.trim()) return;
      const id = uuidv4();
      await dbRun("INSERT INTO channels (id, name, isPrivate, ownerId) VALUES (?, ?, ?, ?)", [id, name, isPrivate ? 1 : 0, clientInfo.userId]);
      // Добавляем создателя
      await dbRun("INSERT INTO channel_members (channelId, userId) VALUES (?, ?)", [id, clientInfo.userId]);
      // Уведомляем всех клиентов об обновлении списка каналов
      broadcastChannelsList();
      sendTo(ws, { type: 'channel_created', payload: { id, name, isPrivate } });
      return;
    }

    // --- ПРИГЛАШЕНИЕ В ПРИВАТНЫЙ КАНАЛ ---
    if (type === 'invite_to_channel') {
      const { channelId, userName } = payload;
      // Проверить, что пользователь - владелец канала
      const channel = await dbGet("SELECT ownerId FROM channels WHERE id = ?", [channelId]);
      if (!channel || channel.ownerId !== clientInfo.userId) {
        sendTo(ws, { type: 'error', payload: 'Только владелец может приглашать' });
        return;
      }
      const invitedUser = await dbGet("SELECT id FROM users WHERE name = ?", [userName]);
      if (!invitedUser) {
        sendTo(ws, { type: 'error', payload: 'Пользователь не найден' });
        return;
      }
      // Проверить, не состоит ли уже
      const existing = await dbGet("SELECT * FROM channel_members WHERE channelId = ? AND userId = ?", [channelId, invitedUser.id]);
      if (existing) {
        sendTo(ws, { type: 'error', payload: 'Уже в канале' });
        return;
      }
      await dbRun("INSERT INTO channel_members (channelId, userId) VALUES (?, ?)", [channelId, invitedUser.id]);
      // Уведомить приглашённого, если онлайн
      for (const [clientWs, info] of clients) {
        if (info && info.userId === invitedUser.id) {
          sendChannelsList(clientWs);
          // Если он в этом канале, обновить историю
          if (info.channelId === channelId) {
            sendChannelHistory(clientWs, channelId);
          }
        }
      }
      sendTo(ws, { type: 'invite_success', payload: { channelId, userName } });
      return;
    }

    // --- ПЕЧАТАЕТ (typing) ---
    if (type === 'typing') {
      const channelId = clientInfo.channelId;
      if (!channelId) return;
      const user = await dbGet("SELECT name FROM users WHERE id = ?", [clientInfo.userId]);
      broadcastToChannel(channelId, { type: 'typing', payload: { userId: clientInfo.userId, userName: user.name } }, ws);
      // Очищаем таймаут
      if (clientInfo.typingTimeout) clearTimeout(clientInfo.typingTimeout);
      clientInfo.typingTimeout = setTimeout(() => {
        broadcastToChannel(channelId, { type: 'typing_stop', payload: { userId: clientInfo.userId } }, null);
      }, 3000);
      return;
    }

    // --- ВЫХОД (logout) обрабатывается на клиенте, просто закрываем сокет ---
  });

  ws.on('close', () => {
    clients.delete(ws);
  });
});

// --- Вспомогательные функции отправки ---

async function sendChannelsList(ws) {
  const channels = await dbAll(`
    SELECT c.id, c.name, c.isPrivate,
      (SELECT COUNT(*) FROM channel_members WHERE channelId = c.id) as memberCount
    FROM channels c
    ORDER BY c.name
  `);
  // Добавим флаг, состоит ли пользователь в канале
  const userId = clients.get(ws)?.userId;
  if (userId) {
    for (const ch of channels) {
      const member = await dbGet("SELECT * FROM channel_members WHERE channelId = ? AND userId = ?", [ch.id, userId]);
      ch.isMember = !!member;
    }
  }
  sendTo(ws, { type: 'channels_list', payload: channels });
}

async function broadcastChannelsList() {
  for (const [ws, info] of clients) {
    if (info && info.userId) {
      sendChannelsList(ws);
    }
  }
}

async function sendChannelHistory(ws, channelId) {
  if (!channelId) return;
  const messages = await dbAll(
    "SELECT id, userId, userName, text, timestamp FROM messages WHERE channelId = ? ORDER BY timestamp ASC LIMIT 100",
    [channelId]
  );
  sendTo(ws, { type: 'channel_history', payload: { channelId, messages } });
  // Также отправить текущий список участников (для отображения)
  const members = await dbAll(`
    SELECT u.id, u.name FROM users u
    JOIN channel_members cm ON cm.userId = u.id
    WHERE cm.channelId = ?
  `, [channelId]);
  sendTo(ws, { type: 'channel_members', payload: { channelId, members } });
}

console.log('🚀 Nexa server running on ws://localhost:8080');
