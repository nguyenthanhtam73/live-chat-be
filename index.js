const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const mysql = require('mysql2/promise');
const AWS = require('aws-sdk');
const multer = require('multer');
const fs = require('fs');
require('dotenv').config(); // Thêm dòng này để load .env

const app = express();
const server = http.createServer(app);
const io = new Server(server, { cors: { origin: '*' } });

// Cấu hình S3
const s3 = new AWS.S3({
  accessKeyId: process.env.S3_ACCESS_KEY, // Lấy từ .env
  secretAccessKey: process.env.S3_SECRET_KEY, // Lấy từ .env
  region: 'us-east-1',
});
const upload = multer({ dest: 'uploads/' });

// Kết nối RDS
const db = mysql.createPool({
  host: process.env.DB_HOST, // Lấy từ .env
  user: process.env.DB_USERNAME,
  password: process.env.DB_PASSWORD,
  database: process.env.DB_DATABASE,
  port: process.env.DB_PORT,
});

// Test kết nối RDS
async function testConnection() {
  try {
    const connection = await db.getConnection();
    console.log('Connected to RDS!');
    connection.release();
  } catch (err) {
    console.error('Error connecting to RDS:', err);
  }
}
testConnection();

// Upload file lên S3
async function uploadFileToS3(file) {
  const params = {
    Bucket: 'live-chat-s3-2025', // Thay bằng bucket của mày nếu khác
    Key: `${Date.now()}-${file.originalname}`,
    Body: fs.createReadStream(file.path),
    ContentType: file.mimetype,
    ACL: 'public-read',
  };
  const { Location } = await s3.upload(params).promise();
  fs.unlinkSync(file.path); // Xóa file tạm
  return Location;
}

// API gửi tin nhắn có file (không bắt buộc file)
app.post('/send-message', upload.single('file'), async (req, res) => {
  try {
    const { senderId, receiverId, message } = req.body;
    let fileId = null;

    // Check senderId và receiverId có tồn tại trong bảng users không
    const [sender] = await db.query('SELECT id FROM users WHERE id = ?', [senderId]);
    const [receiver] = await db.query('SELECT id FROM users WHERE id = ?', [receiverId]);
    if (!sender.length || !receiver.length) {
      return res.status(400).send('Sender or receiver does not exist');
    }

    // Nếu có file, upload lên S3 và lưu vào bảng files
    if (req.file) {
      const fileUrl = await uploadFileToS3(req.file);
      const [result] = await db.query(
        'INSERT INTO files (file_url, file_type) VALUES (?, ?)',
        [fileUrl, req.file.mimetype]
      );
      fileId = result.insertId;
    }

    // Insert tin nhắn, file_id có thể là NULL nếu không có file
    const [msgResult] = await db.query(
      'INSERT INTO messages (sender_id, receiver_id, message, file_id) VALUES (?, ?, ?, ?)',
      [senderId, receiverId, message || '', fileId]
    );

    // Lấy tin nhắn vừa gửi để trả về real-time
    const [newMsg] = await db.query(
      'SELECT m.*, f.file_url FROM messages m LEFT JOIN files f ON m.file_id = f.id WHERE m.id = ?',
      [msgResult.insertId]
    );

    io.emit('chat message', newMsg[0]); // Gửi real-time qua Socket.IO
    res.status(200).send('Message sent');
  } catch (err) {
    console.error('Error in /send-message:', err.message);
    res.status(500).send('Server error');
  }
});

// API lấy tin nhắn
app.get('/messages/:userId', async (req, res) => {
  const { userId } = req.params;
  try {
    const [rows] = await db.query(
      'SELECT m.*, f.file_url FROM messages m LEFT JOIN files f ON m.file_id = f.id WHERE sender_id = ? OR receiver_id = ? ORDER BY sent_at ASC',
      [userId, userId]
    );
    res.json(rows);
  } catch (err) {
    res.status(500).send('Error fetching messages');
  }
});

// Socket.IO real-time
io.on('connection', (socket) => {
  console.log('User connected');
  socket.on('disconnect', () => console.log('User disconnected'));
});

server.listen(4000, () => console.log('Backend running on port 4000'));