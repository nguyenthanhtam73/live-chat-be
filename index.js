const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const mysql = require('mysql2/promise');
const AWS = require('aws-sdk');
const multer = require('multer');
const fs = require('fs');
const cors = require('cors'); // Thêm cors
require('dotenv').config();

const app = express();
const server = http.createServer(app);
const io = new Server(server, { cors: { origin: '*' } });

// Áp dụng CORS cho tất cả route
app.use(cors({
  origin: '*', // Cho phép tất cả origin, hoặc chỉ rõ 'http://localhost:3000'
  methods: ['GET', 'POST'], // Các method FE dùng
  allowedHeaders: ['Content-Type'], // Header FE gửi
}));

const s3 = new AWS.S3({
  accessKeyId: process.env.S3_ACCESS_KEY,
  secretAccessKey: process.env.S3_SECRET_KEY,
  region: 'us-east-1',
});
const upload = multer({ dest: 'uploads/' });

const db = mysql.createPool({
  host: process.env.DB_HOST,
  user: process.env.DB_USERNAME,
  password: process.env.DB_PASSWORD,
  database: process.env.DB_DATABASE,
  port: process.env.DB_PORT,
});

async function testConnection() {
  try {
    const connection = await db.getConnection();
    console.log('Connected to RDS!');
    connection.release();
  } catch (err) {
    console.error('Error connecting to RDS:', err.message);
    process.exit(1);
  }
}
testConnection();

async function uploadFileToS3(file) {
  const params = {
    Bucket: 'live-chat-s3-2025',
    Key: `${Date.now()}-${file.originalname}`,
    Body: fs.createReadStream(file.path),
    ContentType: file.mimetype,
    ACL: 'public-read',
  };
  const { Location } = await s3.upload(params).promise();
  fs.unlinkSync(file.path);
  return Location;
}

app.post('/send-message', upload.single('file'), async (req, res) => {
  try {
    const { senderId, receiverId, message } = req.body;
    let fileId = null;

    const [sender] = await db.query('SELECT id FROM users WHERE id = ?', [senderId]);
    const [receiver] = await db.query('SELECT id FROM users WHERE id = ?', [receiverId]);
    if (!sender.length || !receiver.length) {
      return res.status(400).send('Sender or receiver does not exist');
    }

    if (req.file) {
      const fileUrl = await uploadFileToS3(req.file);
      const [result] = await db.query(
        'INSERT INTO files (file_url, file_type) VALUES (?, ?)',
        [fileUrl, req.file.mimetype]
      );
      fileId = result.insertId;
    }

    const [msgResult] = await db.query(
      'INSERT INTO messages (sender_id, receiver_id, message, file_id) VALUES (?, ?, ?, ?)',
      [senderId, receiverId, message || '', fileId]
    );

    const [newMsg] = await db.query(
      'SELECT m.*, f.file_url FROM messages m LEFT JOIN files f ON m.file_id = f.id WHERE m.id = ?',
      [msgResult.insertId]
    );
    io.emit('chat message', newMsg[0]);
    res.status(200).send('Message sent');
  } catch (err) {
    console.error('Error in /send-message:', err.message);
    res.status(500).send('Server error');
  }
});

server.listen(4000, () => console.log('Backend running on port 4000'));