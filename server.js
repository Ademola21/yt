const express = require('express');
const { exec } = require('child_process');
const { v4: uuidv4 } = require('uuid');
const path = require('path');
const fs = require('fs');
const cors = require('cors');
require('dotenv').config();
const { db, apiKeys } = require('./db');
const { eq, desc } = require('drizzle-orm');

const app = express();
const PORT = process.env.PORT || 4000;

// Use custom ffmpeg with libfdk-aac support
const FFMPEG_PATH = path.join(__dirname, 'node_modules', 'ffmpeg-for-homebridge', 'ffmpeg');

console.log('Database connection initialized.');

// --- Middleware ---
app.use(cors());
app.use(express.json());

const authenticateKey = async (req, res, next) => {
  const authHeader = req.headers['authorization'];
  const token = authHeader && authHeader.split(' ')[1];

  if (token == null) {
    return res.status(401).json({ error: 'Unauthorized: No token provided' });
  }

  try {
    const [existingKey] = await db.select().from(apiKeys).where(eq(apiKeys.key, token));
    
    if (!existingKey) {
      return res.status(403).json({ error: 'Forbidden: Invalid API key. Please generate a key from the dashboard.' });
    }

    next();
  } catch (error) {
    console.error('Error authenticating API key:', error);
    return res.status(500).json({ error: 'Internal server error during authentication' });
  }
};

// --- Helper Functions ---
const { spawn } = require('child_process');

const runCommand = (command, args = []) => {
  return new Promise((resolve, reject) => {
    const process = spawn(command, args);
    let stdout = '';
    let stderr = '';

    process.stdout.on('data', (data) => {
      stdout += data.toString();
    });

    process.stderr.on('data', (data) => {
      stderr += data.toString();
    });

    process.on('close', (code) => {
      if (code !== 0) {
        console.error(`Command failed: ${command} ${args.join(' ')}\n${stderr}`);
        reject(new Error(`Execution failed: ${stderr}`));
        return;
      }
      resolve(stdout);
    });
  });
};

const generateApiKey = () => {
  const characters = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789';
  let result = 'vpa_';
  for (let i = 0; i < 32; i++) {
    result += characters.charAt(Math.floor(Math.random() * characters.length));
  }
  return result;
};

// --- API Endpoints ---

// Root endpoint
app.get('/', (req, res) => {
  res.json({ 
    message: 'Video Download API Server',
    version: '1.0.0',
    endpoints: {
      'POST /v1/keys': 'Generate a new API key',
      'POST /v1/download': 'Download and merge video (requires API key)',
      'POST /v1/formats': 'Get available video formats and file sizes'
    }
  });
});

// Endpoint to get all API keys
app.get('/v1/keys', async (req, res) => {
  try {
    const allKeys = await db.select().from(apiKeys).orderBy(desc(apiKeys.createdAt));
    res.json({ keys: allKeys });
  } catch (error) {
    console.error('Error fetching API keys:', error);
    res.status(500).json({ error: 'Failed to fetch API keys' });
  }
});

// Endpoint to generate a new API key (open for development, no master key required)
app.post('/v1/keys', async (req, res) => {
  try {
    const newKey = generateApiKey();
    const [insertedKey] = await db.insert(apiKeys).values({ key: newKey }).returning();
    
    console.log(`Generated new API key: ${newKey.substring(0, 8)}...`);
    res.status(201).json({ key: insertedKey.key, id: insertedKey.id, createdAt: insertedKey.createdAt });
  } catch (error) {
    console.error('Error creating API key:', error);
    res.status(500).json({ error: 'Failed to generate API key' });
  }
});

// Endpoint to get available video formats
app.post('/v1/formats', authenticateKey, async (req, res) => {
  const { url } = req.body;

  if (!url) {
    return res.status(400).json({ error: 'Missing "url" in request body' });
  }

  try {
    console.log(`Fetching formats for URL: ${url}`);
    const output = await runCommand('yt-dlp', ['-J', url]);
    const videoInfo = JSON.parse(output);

    // Calculate merged audio stream size based on the actual encoding settings we use
    // Default is libfdk_aac at 30k bitrate
    const targetAudioBitrate = 30; // 30 kbps as per default in /v1/download
    let audioSize = 0;
    if (videoInfo.duration) {
      // Calculate audio size based on target bitrate: (bitrate_kbps * 1000 / 8) * duration
      audioSize = Math.round((targetAudioBitrate * 1000 / 8) * videoInfo.duration);
    }

    // Extract and format available video qualities (only MP4/H.264 compatible)
    const formats = videoInfo.formats
      .filter(f => {
        // Only video formats with height, and MP4 container with H.264 codec
        return f.vcodec !== 'none' && 
               f.height && 
               (f.ext === 'mp4' || f.vcodec?.includes('avc') || f.vcodec?.includes('h264'));
      })
      .map(f => {
        // Calculate video stream size - prefer exact values from yt-dlp
        let videoSize = f.filesize || f.filesize_approx || 0;
        
        // If no exact filesize available, estimate from bitrate and duration
        if (videoSize === 0 && videoInfo.duration) {
          // For progressive formats (video+audio), use tbr (total bitrate)
          // For adaptive formats (video-only), use vbr (video bitrate)
          const bitrate = (f.acodec === 'none') ? (f.vbr || f.tbr) : f.tbr;
          if (bitrate) {
            videoSize = Math.round((bitrate * 1000 / 8) * videoInfo.duration);
          }
        }
        
        // For video-only formats (adaptive), add audio stream size for merged estimate
        // For progressive formats (already includes audio), use as-is
        let totalSize = (f.acodec === 'none') ? videoSize + audioSize : videoSize;
        
        // Apply compression factor - FFmpeg merging with copy codec and re-encoding audio
        // at 30kbps results in ~40% smaller file due to container optimization
        // and significantly lower audio bitrate than source
        totalSize = Math.round(totalSize * 0.60);
        
        return {
          format_id: f.format_id,
          resolution: `${f.height}p`,
          height: f.height,
          fps: f.fps || 30,
          filesize: totalSize,
          ext: f.ext,
          vcodec: f.vcodec,
          acodec: f.acodec
        };
      })
      .sort((a, b) => a.height - b.height); // Sort by resolution

    // Group by resolution and keep the best MP4 format for each resolution
    const uniqueFormats = [];
    const seenHeights = new Set();
    
    for (const format of formats) {
      if (!seenHeights.has(format.height)) {
        seenHeights.add(format.height);
        uniqueFormats.push(format);
      }
    }

    res.json({
      title: videoInfo.title,
      duration: videoInfo.duration,
      thumbnail: videoInfo.thumbnail,
      formats: uniqueFormats
    });

  } catch (error) {
    console.error(`Error fetching formats:`, error.message);
    res.status(500).json({ error: 'Failed to fetch video formats' });
  }
});

app.post('/v1/download', authenticateKey, async (req, res) => {
  const { url, format_id, audio_format = 'libfdk_aac', audio_bitrate = '30k' } = req.body;

  if (!url) {
    return res.status(400).json({ error: 'Missing "url" in request body' });
  }
  
  const jobId = uuidv4();
  const tempDir = path.join(__dirname, 'temp', jobId);
  const videoPath = path.join(tempDir, 'video.mp4');
  const audioPath = path.join(tempDir, 'audio.m4a');

  try {
    await fs.promises.mkdir(tempDir, { recursive: true });

    console.log(`[${jobId}] Starting download for URL: ${url}`);
    
    // Get video metadata to retrieve the title
    console.log(`[${jobId}] Fetching video metadata...`);
    const metadataOutput = await runCommand('yt-dlp', ['-J', url]);
    const videoInfo = JSON.parse(metadataOutput);
    const videoTitle = videoInfo.title || 'video';
    
    // Sanitize filename - remove invalid characters for filesystem
    const sanitizedTitle = videoTitle.replace(/[<>:"/\\|?*\x00-\x1F]/g, '_').substring(0, 200);
    const outputPath = path.join(tempDir, `${sanitizedTitle}.mp4`);
    
    // Use specific format if provided, otherwise use best quality
    const videoFormat = format_id ? format_id : 'bestvideo[ext=mp4]/bestvideo';
    await runCommand('yt-dlp', ['-f', videoFormat, '-o', videoPath, url]);

    await runCommand('yt-dlp', ['-f', 'bestaudio[ext=m4a]/bestaudio', '-o', audioPath, url]);

    console.log(`[${jobId}] Merging files with FFmpeg (HE-AAC encoding)...`);
    // Use HE-AAC (AAC LC SBR) with variable bitrate mode
    // -profile:a aac_he enables HE-AAC (mp4a-40-5)
    // -vbr 2 enables variable bitrate mode (1-5 scale, 2 is high quality for 30kbps)
    await runCommand(FFMPEG_PATH, [
      '-i', videoPath,
      '-i', audioPath,
      '-c:v', 'copy',
      '-c:a', audio_format,
      '-profile:a', 'aac_he',
      '-vbr', '2',
      '-metadata:s:a:0', 'language=eng',
      outputPath
    ]);

    console.log(`[${jobId}] Sending file to client: ${sanitizedTitle}.mp4`);
    
    // Stream the file directly to the client for faster download start
    const fileStats = await fs.promises.stat(outputPath);
    res.setHeader('Content-Type', 'video/mp4');
    res.setHeader('Content-Length', fileStats.size);
    
    // Properly encode filename for Content-Disposition to support Unicode/emoji
    // Use RFC 5987 encoding with ASCII fallback for compatibility
    const asciiSafeName = sanitizedTitle.replace(/[^\x20-\x7E]/g, '_') + '.mp4';
    const encodedFilename = encodeURIComponent(sanitizedTitle + '.mp4').replace(/['()]/g, escape).replace(/\*/g, '%2A');
    res.setHeader('Content-Disposition', `attachment; filename="${asciiSafeName}"; filename*=UTF-8''${encodedFilename}`);
    
    const fileStream = fs.createReadStream(outputPath);
    fileStream.pipe(res);
    
    fileStream.on('end', () => {
      console.log(`[${jobId}] File sent successfully. Cleaning up temporary files.`);
      fs.rm(tempDir, { recursive: true, force: true }, (cleanupErr) => {
        if (cleanupErr) {
          console.error(`[${jobId}] Error during cleanup:`, cleanupErr);
        }
      });
    });
    
    fileStream.on('error', (err) => {
      console.error(`[${jobId}] Error streaming file:`, err);
      fs.rm(tempDir, { recursive: true, force: true }, () => {});
    });

  } catch (error) {
    console.error(`[${jobId}] An error occurred:`, error.message);
    if (!res.headersSent) {
      res.status(500).json({ error: 'An internal server error occurred during video processing.' });
    }
    
    fs.rm(tempDir, { recursive: true, force: true }, () => {});
  }
});

app.listen(PORT, () => {
  console.log(`Backend server is running on http://localhost:${PORT}`);
});