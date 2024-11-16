const mysql = require('mysql2/promise');
const admin = require('firebase-admin');
const path = require('path');

// Firebase initialization
function initializeFirebase(credentialsPath) {
  const serviceAccount = require(credentialsPath);
  admin.initializeApp({
    credential: admin.credential.cert(serviceAccount)
  });
}

// Process data for Firestore
function processData(row) {
  const processedRow = {};
  for (const [key, value] of Object.entries(row)) {
    if (value instanceof Date) {
      processedRow[key] = admin.firestore.Timestamp.fromDate(value);
    } else if (typeof value === 'bigint') {
      processedRow[key] = value.toString();
    } else if (Buffer.isBuffer(value)) {
      processedRow[key] = value.toString('base64');
    } else {
      processedRow[key] = value;
    }
  }
  return processedRow;
}

// Delay helper function
const delay = ms => new Promise(resolve => setTimeout(resolve, ms));

// Upload a single batch to Firestore with retry logic
async function uploadBatch(db, collection, docs, batchNumber, retries = 3) {
  let attempt = 0;
  while (attempt < retries) {
    try {
      const batch = db.batch();
      docs.forEach((doc) => {
        const docRef = db.collection('fcc_amateur_aamir').doc();
        batch.set(docRef, doc);
      });
      await batch.commit();
      console.log(`Batch ${batchNumber}: Successfully uploaded ${docs.length} documents`);
      return true;
    } catch (error) {
      attempt++;
      if (attempt === retries) {
        throw error;
      }
      console.log(`Batch ${batchNumber}: Attempt ${attempt} failed, retrying after delay...`);
      await delay(Math.pow(2, attempt) * 1000); // Exponential backoff
    }
  }
}

// Upload data to Firestore with chunking
async function uploadToFirestore(data, collection) {
  const db = admin.firestore();
  const batchSize = 250; // Reduced batch size
  const totalBatches = Math.ceil(data.length / batchSize);
  const failures = [];

  console.log(`Starting upload of ${data.length} records in ${totalBatches} batches`);

  for (let i = 0; i < data.length; i += batchSize) {
    const batchNumber = Math.floor(i / batchSize) + 1;
    const chunk = data.slice(i, i + batchSize).map(processData);

    try {
      await uploadBatch(db, collection, chunk, batchNumber);
      
      // Add a small delay between batches to prevent overloading
      await delay(500);
      
      if (batchNumber % 10 === 0) {
        console.log(`Progress: ${batchNumber}/${totalBatches} batches completed`);
      }
    } catch (error) {
      console.error(`Failed to upload batch ${batchNumber}:`, error);
      failures.push({
        batchNumber,
        startIndex: i,
        endIndex: i + chunk.length
      });
    }
  }

  return failures;
}

// Fetch data from MySQL in chunks - FIXED VERSION
async function* fetchMySQLDataInChunks(connection, table, chunkSize = 10000) {
  let offset = 0;
  let hasMore = true;

  while (hasMore) {
    const query = `SELECT * FROM ${table} LIMIT ${chunkSize} OFFSET ${offset}`;
    const [rows] = await connection.query(query);

    if (rows.length > 0) {
      yield rows;
      offset += rows.length;
      console.log(`Fetched ${offset} records from MySQL`);
    }

    hasMore = rows.length === chunkSize;
  }
}

// Main migration function
async function migrateToFirestore() {
  let connection;

  try {
    // MySQL connection
    const mysqlConfig = {
      host: 'localhost',
      user: 'aamir',
      password: '1113@Aamir',
      database: 'fcc_amateur'
    };

    connection = await mysql.createConnection(mysqlConfig);
    console.log('Connected to MySQL database');

    const credentialsPath = path.join(__dirname, './serviceAccount.json');
    initializeFirebase(credentialsPath);
    console.log('Firestore initialized');

    const tablesToMigrate = ['en']; // Update with your table name

    for (const table of tablesToMigrate) {
      console.log(`Starting migration of table: ${table}`);
      
      const failures = [];
      
      // Process data in chunks
      for await (const chunk of fetchMySQLDataInChunks(connection, table)) {
        const chunkFailures = await uploadToFirestore(chunk, table);
        failures.push(...chunkFailures);
      }

      // Report any failures
      if (failures.length > 0) {
        console.log('Failed batches:', failures);
        console.log('Please run the migration again for these specific batches');
      }
    }

  } catch (error) {
    console.error('Migration failed:', error);
    throw error;
  } finally {
    if (connection) {
      await connection.end();
      console.log('MySQL connection closed');
    }
  }
}

// Run the migration
migrateToFirestore().catch(console.error);

// Handle interruptions
process.on('SIGINT', async () => {
  console.log('Migration interrupted');
  process.exit(1);
});