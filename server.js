/**
 * ==========================================================
 * GOLDING LIBRARY BACKBLAZE B2 SERVER - ENHANCED & FIXED
 * With robust error handling and connection resilience
 * ==========================================================
 */

const express = require("express");
const cors = require("cors");
const dotenv = require("dotenv");
const multer = require("multer");
const path = require("path");
const fs = require("fs");
const {
  S3Client,
  ListObjectsV2Command,
  GetObjectCommand,
  PutObjectCommand,
  DeleteObjectCommand,
  HeadObjectCommand,
  CopyObjectCommand
} = require("@aws-sdk/client-s3");
const { getSignedUrl } = require("@aws-sdk/s3-request-presigner");

dotenv.config();

const app = express();
const PORT = process.env.PORT || 5500;

// Enhanced CORS for all frontend origins
app.use(cors({
  origin: ['http://localhost:5500', 'http://127.0.0.1:5500', 'http://localhost:3000', '*'],
  methods: ['GET', 'POST', 'DELETE', 'PUT'],
  allowedHeaders: ['Content-Type', 'Authorization'],
  credentials: true
}));

app.use(express.json({ limit: '50mb' }));
app.use(express.urlencoded({ extended: true, limit: '50mb' }));
app.use(express.static(path.join(__dirname)));

/* ==========================================================
   ENHANCED STORAGE & CLIENT CONFIGURATION WITH RETRY LOGIC
========================================================== */
const storage = multer.memoryStorage();
const upload = multer({ 
  storage,
  limits: { 
    fileSize: 100 * 1024 * 1024,
    files: 10
  },
  fileFilter: (req, file, cb) => {
    const allowedTypes = [
      'application/pdf',
      'video/mp4', 'video/mpeg', 'video/quicktime', 'video/x-msvideo',
      'audio/mpeg', 'audio/wav', 'audio/x-wav',
      'image/jpeg', 'image/png', 'image/gif',
      'application/msword',
      'application/vnd.openxmlformats-officedocument.wordprocessingml.document',
      'text/plain',
      'application/json'
    ];
    
    if (allowedTypes.includes(file.mimetype) || file.mimetype.startsWith('video/') || file.mimetype.startsWith('audio/')) {
      cb(null, true);
    } else {
      cb(new Error(`Unsupported file type: ${file.mimetype}`), false);
    }
  }
});

// ENHANCED S3 Client with robust error handling and retries
const s3Client = new S3Client({
  region: process.env.B2_REGION || 'us-east-005',
  endpoint: process.env.B2_S3_ENDPOINT || 'https://s3.us-east-005.backblazeb2.com',
  credentials: {
    accessKeyId: process.env.B2_APPLICATION_KEY_ID || '00513f2439c0e900000000002',
    secretAccessKey: process.env.B2_APPLICATION_KEY || 'K0056z7N8S9Qh1P0N1Q2X3Y4Z5a6b7c'
  },
  maxAttempts: 5,
  retryMode: 'adaptive',
  requestTimeout: 30000,
  connectionTimeout: 10000
});

// Fallback bucket name
const B2_BUCKET_NAME = process.env.B2_BUCKET_NAME || 'goldinglibrary';

/* ==========================================================
   CONFIGURATION: FOLDER PATHS FOR ALL PAGES
========================================================== */
const folderPaths = {
  ebooks: "ebooks/",
  journals: "journals/",
  users: "users/",
  projects: "projects/",
  research: "research/", 
  internal_lectures: "internal_lectures/",
  external_lectures: "external_lectures/",
  community: "community/"
};

/* ==========================================================
   ENHANCED HELPER FUNCTIONS WITH RETRY LOGIC
========================================================== */

// Enhanced retry function for B2 operations
async function executeWithRetry(operation, maxRetries = 3, baseDelay = 1000) {
  let lastError;
  
  for (let attempt = 1; attempt <= maxRetries; attempt++) {
    try {
      return await operation();
    } catch (error) {
      lastError = error;
      console.warn(`[RETRY] Attempt ${attempt}/${maxRetries} failed:`, error.message);
      
      if (attempt < maxRetries) {
        const delay = baseDelay * Math.pow(2, attempt - 1); // Exponential backoff
        console.log(`[RETRY] Waiting ${delay}ms before retry...`);
        await new Promise(resolve => setTimeout(resolve, delay));
      }
    }
  }
  
  throw lastError;
}

function sanitizeMetadataKey(key) {
  if (typeof key !== 'string') return '';
  return key
    .toLowerCase()
    .replace(/[^a-z0-9.-]/g, '-')
    .replace(/-+/g, '-')
    .replace(/^-|-$/g, '')
    .substring(0, 50);
}

function sanitizeMetadataValue(value) {
  if (value === null || value === undefined) return '';
  return String(value)
    .replace(/[^\x20-\x7E]/g, '')
    .substring(0, 500);
}

function sanitizeMetadata(metadata) {
  const clean = {};
  if (!metadata || typeof metadata !== 'object') return clean;

  const standardFields = {
    title: 'Untitled Document',
    author: 'Unknown Author',
    description: '',
    department: 'General',
    category: 'General',
    year: new Date().getFullYear().toString(),
    abstract: '',
    keywords: '',
    publisher: 'Sa\'adatu Rimi College',
    language: 'en',
    pages: '0',
    isbn: '',
    doi: '',
    uploadedBy: 'System',
    uploadDate: new Date().toISOString()
  };

  const mergedMetadata = { ...standardFields, ...metadata };

  for (const [key, value] of Object.entries(mergedMetadata)) {
    const cleanKey = sanitizeMetadataKey(key);
    if (cleanKey) {
      clean[cleanKey] = sanitizeMetadataValue(value);
    }
  }

  return clean;
}

// Enhanced metadata cache with timeout
const metadataCache = new Map();
const CACHE_DURATION = 5 * 60 * 1000;

function cleanupCache() {
  const now = Date.now();
  for (const [key, value] of metadataCache.entries()) {
    if (now - value.timestamp > CACHE_DURATION) {
      metadataCache.delete(key);
    }
  }
}

// Run cache cleanup every minute
setInterval(cleanupCache, 60000);

async function getFileMetadata(key) {
  const cacheKey = `${B2_BUCKET_NAME}:${key}`;
  const cached = metadataCache.get(cacheKey);
  
  if (cached && Date.now() - cached.timestamp < CACHE_DURATION) {
    return cached.metadata;
  }

  try {
    const operation = () => {
      const command = new HeadObjectCommand({
        Bucket: B2_BUCKET_NAME,
        Key: key
      });
      return s3Client.send(command);
    };
    
    const response = await executeWithRetry(operation, 2);
    const metadata = response.Metadata || {};
    
    metadataCache.set(cacheKey, {
      metadata,
      timestamp: Date.now()
    });
    
    return metadata;
  } catch (error) {
    console.error(`[METADATA] Failed to fetch metadata for ${key}:`, error.message);
    return {};
  }
}

function determineFileType(key, metadata = {}) {
  const ext = path.extname(key).toLowerCase();
  
  if (metadata.type) return metadata.type;
  if (metadata.category === 'video') return 'video';
  if (metadata.category === 'audio') return 'audio';
  
  const typeMap = {
    '.pdf': 'pdf',
    '.mp4': 'video', '.mov': 'video', '.avi': 'video', '.mkv': 'video', '.webm': 'video',
    '.mp3': 'audio', '.wav': 'audio', '.aac': 'audio', '.flac': 'audio',
    '.jpg': 'image', '.jpeg': 'image', '.png': 'image', '.gif': 'image', '.webp': 'image',
    '.doc': 'document', '.docx': 'document', '.txt': 'text', '.rtf': 'document',
    '.json': 'metadata', '.xml': 'metadata'
  };
  
  if (key.includes('external_lectures') || metadata.url) {
    return 'youtube';
  }
  
  return typeMap[ext] || 'document';
}

function extractDisplayTitle(key, metadata = {}) {
  if (metadata.title && metadata.title !== 'Untitled Document') {
    return metadata.title;
  }
  
  const filename = path.basename(key);
  return filename
    .replace(/\.[^/.]+$/, "")
    .replace(/^\d+-/, '')
    .replace(/[_-]/g, ' ')
    .replace(/\b\w/g, l => l.toUpperCase())
    .trim();
}

function enrichFileInfo(item, metadata = {}, folder = '') {
  const displayTitle = extractDisplayTitle(item.Key, metadata);
  const fileType = determineFileType(item.Key, metadata);
  
  return {
    key: item.Key,
    name: path.basename(item.Key),
    size: item.Size,
    lastModified: item.LastModified,
    type: fileType,
    metadata: metadata,
    title: displayTitle,
    author: metadata.author || 'Unknown Author',
    description: metadata.description || metadata.abstract || `${displayTitle} is available in our collection.`,
    department: metadata.department || inferDepartment(folder),
    category: metadata.category || inferCategory(folder),
    year: metadata.year || new Date(item.LastModified).getFullYear().toString(),
    abstract: metadata.abstract || '',
    studentId: metadata.studentid || metadata.studentId || '',
    supervisor: metadata.supervisor || '',
    institution: metadata.institution || 'Sa\'adatu Rimi College',
    degreeLevel: metadata.degreelevel || metadata.degreeLevel || '',
    contentType: metadata.contenttype || metadata.contentType || 'academic',
    displaySize: formatFileSize(item.Size),
    displayDate: new Date(item.LastModified).toLocaleDateString('en-US'),
    displayType: fileType.toUpperCase(),
    url: null
  };
}

function inferDepartment(folder) {
  const departmentMap = {
    'ebooks': 'Library',
    'journals': 'Research',
    'projects': 'Student Affairs',
    'research': 'Academic Research',
    'internal-lectures': 'Faculty',
    'external-lectures': 'External Relations',
    'community': 'Community Engagement',
    'users': 'Administration'
  };
  return departmentMap[folder] || 'General Studies';
}

function inferCategory(folder) {
  return folder ? folder.replace(/s\/$/, '').replace(/_/g, ' ').replace(/\b\w/g, l => l.toUpperCase()) : 'General';
}

function formatFileSize(bytes) {
  if (bytes === 0) return '0 Bytes';
  const k = 1024;
  const sizes = ['Bytes', 'KB', 'MB', 'GB'];
  const i = Math.floor(Math.log(bytes) / Math.log(k));
  return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
}

/* ==========================================================
   ENHANCED API ENDPOINTS WITH ROBUST ERROR HANDLING
========================================================== */

/**
 * GET /api/files/:folder - Enhanced with retry logic
 */
app.get("/api/files/:folder", async (req, res) => {
  const { folder } = req.params;
  const prefix = folderPaths[folder];
  
  if (!prefix) {
    return res.status(400).json({ 
      error: "Invalid folder path",
      availableFolders: Object.keys(folderPaths)
    });
  }

  try {
    console.log(`[API] Listing files for folder: ${folder} (prefix: ${prefix})`);
    
    const operation = () => {
      const command = new ListObjectsV2Command({
        Bucket: B2_BUCKET_NAME,
        Prefix: prefix,
        MaxKeys: 1000
      });
      return s3Client.send(command);
    };

    const data = await executeWithRetry(operation, 3);
    
    if (!data.Contents || data.Contents.length === 0) {
      console.log(`[API] No files found in ${folder}`);
      return res.json([]);
    }

    // Process files with enhanced metadata
    const files = await Promise.all(
      data.Contents
        .filter(item => !item.Key.endsWith("/"))
        .map(async (item) => {
          try {
            const metadata = await getFileMetadata(item.Key);
            const enrichedFile = enrichFileInfo(item, metadata, folder);
            
            // Generate signed URL with retry
            const urlOperation = () => {
              const urlCommand = new GetObjectCommand({
                Bucket: B2_BUCKET_NAME,
                Key: item.Key
              });
              return getSignedUrl(s3Client, urlCommand, { 
                expiresIn: 3600 
              });
            };
            
            enrichedFile.url = await executeWithRetry(urlOperation, 2);
            
            return enrichedFile;
          } catch (error) {
            console.error(`[API] Error processing file ${item.Key}:`, error.message);
            // Return basic file info even if metadata fails
            const fallbackFile = enrichFileInfo(item, {}, folder);
            fallbackFile.url = `https://f005.backblazeb2.com/file/${B2_BUCKET_NAME}/${item.Key}`;
            return fallbackFile;
          }
        })
    );

    console.log(`[API] Returning ${files.length} enriched files for ${folder}`);
    res.json(files);
  } catch (error) {
    console.error(`[API] Error listing files for ${folder}:`, error.message);
    
    // Return empty array instead of error for better UX
    res.json([]);
  }
});

/**
 * GET /api/search/:query - Enhanced search
 */
app.get("/api/search/:query", async (req, res) => {
  const { query } = req.params;
  const { type = 'all' } = req.query;

  if (!query || query.trim().length < 2) {
    return res.status(400).json({ error: "Query must be at least 2 characters" });
  }

  try {
    console.log(`[SEARCH] Performing search for: "${query}" (type: ${type})`);
    
    let searchFolders = Object.keys(folderPaths);
    if (type !== 'all') {
      searchFolders = [type];
    }

    const allResults = [];
    const searchTerm = query.toLowerCase().trim();
    
    // Search across folders with timeout protection
    const searchPromises = searchFolders.map(async (folder) => {
      try {
        const response = await fetch(`http://localhost:${PORT}/api/files/${folder}`, {
          signal: AbortSignal.timeout(10000) // 10 second timeout
        });
        
        if (!response.ok) {
          console.warn(`[SEARCH] Failed to fetch ${folder}: ${response.status}`);
          return [];
        }
        
        const files = await response.json();
        
        return files.filter(file => {
          const searchableText = [
            file.title,
            file.author,
            file.description,
            file.department,
            file.category,
            file.abstract,
            file.metadata?.keywords || ''
          ].join(' ').toLowerCase();
          
          return searchableText.includes(searchTerm);
        }).map(file => ({
          ...file,
          sourceFolder: folder
        }));
      } catch (error) {
        console.error(`[SEARCH] Error searching folder ${folder}:`, error.message);
        return [];
      }
    });

    const results = await Promise.allSettled(searchPromises);
    
    results.forEach(result => {
      if (result.status === 'fulfilled') {
        allResults.push(...result.value);
      }
    });

    console.log(`[SEARCH] Found ${allResults.length} results for "${query}"`);
    res.json(allResults);
  } catch (error) {
    console.error('[SEARCH] Error:', error.message);
    res.status(500).json({ 
      error: "Search failed",
      details: error.message 
    });
  }
});

/**
 * GET /api/file/:key - Enhanced with fallback URL
 */
app.get("/api/file/:key(*)", async (req, res) => {
  const { key } = req.params;

  try {
    const operation = () => {
      const command = new GetObjectCommand({
        Bucket: B2_BUCKET_NAME,
        Key: key
      });
      return getSignedUrl(s3Client, command, { 
        expiresIn: 7200
      });
    };

    const signedUrl = await executeWithRetry(operation, 2);
    const metadata = await getFileMetadata(key);
    
    res.json({ 
      url: signedUrl,
      metadata: metadata,
      key: key,
      name: path.basename(key),
      fallbackUrl: `https://f005.backblazeb2.com/file/${B2_BUCKET_NAME}/${key}`
    });
  } catch (error) {
    console.error("[SIGNED URL] Error:", error.message);
    
    // Provide fallback URL
    res.json({
      url: null,
      fallbackUrl: `https://f005.backblazeb2.com/file/${B2_BUCKET_NAME}/${key}`,
      key: key,
      name: path.basename(key),
      error: "Signed URL failed, using direct URL"
    });
  }
});

/**
 * POST /api/upload/:folder - Enhanced upload
 */
app.post("/api/upload/:folder", upload.single("file"), async (req, res) => {
  const { folder } = req.params;
  const file = req.file;

  if (!file) {
    return res.status(400).json({ error: "No file uploaded" });
  }

  const prefix = folderPaths[folder];
  if (!prefix) {
    return res.status(400).json({ 
      error: "Invalid folder",
      availableFolders: Object.keys(folderPaths)
    });
  }

  let userMetadata = {};
  if (req.body.metadata) {
    try {
      userMetadata = JSON.parse(req.body.metadata);
    } catch (e) {
      return res.status(400).json({ error: "Invalid metadata JSON" });
    }
  }

  const enhancedMetadata = {
    ...userMetadata,
    originalFilename: file.originalname,
    fileSize: file.size.toString(),
    fileType: file.mimetype,
    uploadedAt: new Date().toISOString(),
    uploader: userMetadata.uploadedBy || 'System'
  };

  const cleanMetadata = sanitizeMetadata(enhancedMetadata);

  const timestamp = Date.now();
  const safeFileName = file.originalname.replace(/[^a-zA-Z0-9._-]/g, '_');
  const key = `${prefix}${timestamp}-${safeFileName}`;

  try {
    const operation = () => {
      const command = new PutObjectCommand({
        Bucket: B2_BUCKET_NAME,
        Key: key,
        Body: file.buffer,
        ContentType: file.mimetype || 'application/octet-stream',
        Metadata: cleanMetadata
      });
      return s3Client.send(command);
    };

    await executeWithRetry(operation, 3);

    // Clear cache for this folder
    const cacheKeys = Array.from(metadataCache.keys()).filter(k => 
      k.includes(`${B2_BUCKET_NAME}:${prefix}`)
    );
    cacheKeys.forEach(k => metadataCache.delete(k));

    res.json({
      success: true,
      key,
      metadata: cleanMetadata,
      url: `https://f005.backblazeb2.com/file/${B2_BUCKET_NAME}/${key}`,
      message: `File uploaded successfully to ${folder}`
    });
  } catch (error) {
    console.error("[UPLOAD] Error:", error.message);
    res.status(500).json({ 
      error: "Upload failed",
      details: error.message 
    });
  }
});

/**
 * DELETE /api/file/:key - Enhanced delete
 */
app.delete("/api/file/:key(*)", async (req, res) => {
  const { key } = req.params;

  try {
    const operation = () => {
      const command = new DeleteObjectCommand({
        Bucket: B2_BUCKET_NAME,
        Key: key
      });
      return s3Client.send(command);
    };
    
    await executeWithRetry(operation, 2);
    
    const cacheKey = `${B2_BUCKET_NAME}:${key}`;
    metadataCache.delete(cacheKey);
    
    res.json({ 
      success: true,
      message: "File deleted successfully"
    });
  } catch (error) {
    console.error("[DELETE] Error:", error.message);
    res.status(500).json({ 
      error: "Failed to delete file",
      details: error.message 
    });
  }
});

/* ==========================================================
   HEALTH CHECK & STATISTICS
========================================================== */

app.get("/", (req, res) => {
  res.json({
    message: "🚀 Golding Library Enhanced Server - FIXED VERSION",
    version: "2.1.0",
    features: [
      "Robust error handling with retry logic",
      "Enhanced metadata handling", 
      "Multi-page compatibility",
      "Connection resilience",
      "Cache management"
    ],
    status: "healthy",
    cacheSize: metadataCache.size,
    timestamp: new Date().toISOString()
  });
});

app.get("/api/status", async (req, res) => {
  try {
    const operation = () => {
      const command = new ListObjectsV2Command({
        Bucket: B2_BUCKET_NAME,
        MaxKeys: 1
      });
      return s3Client.send(command);
    };
    
    await executeWithRetry(operation, 2);
    
    res.json({
      status: "healthy",
      bucket: B2_BUCKET_NAME,
      cacheSize: metadataCache.size,
      timestamp: new Date().toISOString(),
      features: "Enhanced with retry logic"
    });
  } catch (error) {
    res.status(500).json({
      status: "unhealthy",
      error: error.message,
      bucket: B2_BUCKET_NAME
    });
  }
});

/* ==========================================================
   ERROR HANDLING MIDDLEWARE
========================================================== */

app.use((error, req, res, next) => {
  if (error instanceof multer.MulterError) {
    if (error.code === 'LIMIT_FILE_SIZE') {
      return res.status(400).json({ error: 'File too large' });
    }
  }
  
  console.error('[SERVER ERROR]', error);
  res.status(500).json({ 
    error: 'Internal server error',
    ...(process.env.NODE_ENV === 'development' && { details: error.message })
  });
});

app.use((req, res) => {
  res.status(404).json({ error: `Endpoint not found: ${req.method} ${req.path}` });
});

/* ==========================================================
   SERVER START WITH ENHANCED LOGGING
========================================================== */
app.listen(PORT, () => {
  console.log(`\n🚀 Golding Library Enhanced Server (FIXED) running on http://localhost:${PORT}`);
  console.log(`📦 Connected to bucket: ${B2_BUCKET_NAME}`);
  console.log(`🔧 Features: Robust error handling, Retry logic, Connection resilience`);
  console.log(`📁 Available folders: ${Object.keys(folderPaths).join(', ')}`);
  console.log(`💡 Health check: http://localhost:${PORT}/api/status\n`);
  
  // Test connection on startup
  setTimeout(async () => {
    try {
      const testCommand = new ListObjectsV2Command({
        Bucket: B2_BUCKET_NAME,
        MaxKeys: 1
      });
      await s3Client.send(testCommand);
      console.log('✅ Backblaze B2 connection test: SUCCESS');
    } catch (error) {
      console.log('❌ Backblaze B2 connection test: FAILED -', error.message);
    }
  }, 1000);
});
