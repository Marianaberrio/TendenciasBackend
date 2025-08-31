// === api.js (reemplaza TODO) ===
const path = require('path');
require('dotenv').config({ path: path.join(__dirname, '.env') });
const express = require('express');
const sql = require('mssql');

const app = express();
app.use(express.json());

console.log('Boot file:', __filename);
console.log('ENV:', {
  server: process.env.SQL_SERVER,
  db: process.env.SQL_DATABASE,
  user: process.env.SQL_USER,
  encrypt: process.env.SQL_ENCRYPT
});

const sqlConfig = {
  server: process.env.SQL_SERVER,
  database: process.env.SQL_DATABASE,
  user: process.env.SQL_USER,
  password: process.env.SQL_PASSWORD,
  options: { encrypt: process.env.SQL_ENCRYPT === 'true', trustServerCertificate: false },
  pool: { max: 10, min: 0, idleTimeoutMillis: 30000 }
};

let pool;
async function getPool() { if (pool) return pool; pool = await sql.connect(sqlConfig); return pool; }

// raíz
app.get('/', (_req, res) => res.send('API MedPresc ✅'));

// debug conexión
app.get('/debug/db', async (_req, res) => {
  try {
    const p = await getPool();
    const ping = await p.request().query('SELECT 1 AS ok');
    const tables = await p.request().query('SELECT TOP 5 name FROM sys.tables ORDER BY name');
    res.json({ ok: ping.recordset[0].ok, tables: tables.recordset });
  } catch (err) {
    console.error('DEBUG /debug/db:', err);
    res.status(500).json({
      error: 'DB_ERROR',
      name: err.name,
      code: err.code,
      number: err.number,
      message: err.message
    });
  }
});

// lista rutas para confirmar qué está cargado
app.get('/__routes', (_req, res) => {
  const routes = [];
  app._router.stack.forEach((m) => {
    if (m.route && m.route.path) {
      routes.push({ method: Object.keys(m.route.methods)[0].toUpperCase(), path: m.route.path });
    }
  });
  res.json(routes);
});

// =========================
// Pacientes CRUD (/api/pacientes)
// =========================
const PAC_TABLE = '[MedPresc].dbo.[Paciente]';
const PAC_ID = 'id_paciente'; // PK

// Util: construir INSERT dinámico a partir del body
function buildInsert(table, body) {
  const keys = Object.keys(body);
  if (keys.length === 0) throw new Error('EMPTY_BODY');
  const cols = keys.map(k => `[${k}]`).join(', ');
  const params = keys.map((k, i) => `@p${i}`).join(', ');
  return {
    sql: `INSERT INTO ${table} (${cols}) OUTPUT INSERTED.* VALUES (${params});`,
    bind: keys.map((k, i) => ({ name: `p${i}`, value: body[k] }))
  };
}

// Util: construir UPDATE dinámico a partir del body
function buildUpdate(table, idCol, idVal, body) {
  const keys = Object.keys(body).filter(k => k !== idCol);
  if (keys.length === 0) throw new Error('EMPTY_BODY');
  const setClause = keys.map((k, i) => `[${k}] = @p${i}`).join(', ');
  return {
    sql: `
      UPDATE ${table}
      SET ${setClause}
      OUTPUT INSERTED.*
      WHERE [${idCol}] = @id;
    `,
    bind: [
      ...keys.map((k, i) => ({ name: `p${i}`, value: body[k] })),
      { name: 'id', value: idVal }
    ]
  };
}

// POST /api/pacientes  (crear)
app.post('/api/pacientes', async (req, res) => {
  try {
    const p = await getPool();
    const body = { ...req.body };

    const ins = buildInsert(PAC_TABLE, body);
    const r = await ins.bind.reduce((reqAcc, b) => reqAcc.input(b.name, b.value), p.request())
      .query(ins.sql);

    res.status(201).json(r.recordset[0]);
  } catch (err) {
    console.error('POST /api/pacientes:', err);
    if (err.message === 'EMPTY_BODY') {
      return res.status(400).json({ error: 'EMPTY_BODY', message: 'Body vacío.' });
    }
    res.status(500).json({
      error: 'DB_ERROR', name: err.name, code: err.code, number: err.number, message: err.message
    });
  }
});

// GET /api/pacientes  (listar)  ?limit=50&offset=0
app.get('/api/pacientes', async (req, res) => {
  try {
    const p = await getPool();
    const limit = Math.min(parseInt(req.query.limit || '50', 10), 200);
    const offset = parseInt(req.query.offset || '0', 10);

    const q = `
      SELECT *
      FROM ${PAC_TABLE}
      ORDER BY [${PAC_ID}] DESC
      OFFSET @offset ROWS FETCH NEXT @limit ROWS ONLY;
    `;
    const r = await p.request()
      .input('offset', offset)
      .input('limit', limit)
      .query(q);

    res.json(r.recordset);
  } catch (err) {
    console.error('GET /api/pacientes:', err);
    res.status(500).json({
      error: 'DB_ERROR', name: err.name, code: err.code, number: err.number, message: err.message
    });
  }
});

// GET /api/pacientes/:id  (detalle)
app.get('/api/pacientes/:id', async (req, res) => {
  try {
    const p = await getPool();
    const r = await p.request()
      .input('id', req.params.id)
      .query(`
        SELECT TOP 1 *
        FROM ${PAC_TABLE}
        WHERE [${PAC_ID}] = @id;
      `);

    if (r.recordset.length === 0) {
      return res.status(404).json({ error: 'NOT_FOUND' });
    }
    res.json(r.recordset[0]);
  } catch (err) {
    console.error('GET /api/pacientes/:id:', err);
    res.status(500).json({
      error: 'DB_ERROR', name: err.name, code: err.code, number: err.number, message: err.message
    });
  }
});

// PATCH /api/pacientes/:id  (actualización parcial)
app.patch('/api/pacientes/:id', async (req, res) => {
  try {
    const p = await getPool();

    const body = { ...req.body };
    delete body[PAC_ID]; 

    const upd = buildUpdate(PAC_TABLE, PAC_ID, req.params.id, body);

    const reqMs = upd.bind.reduce(
      (reqAcc, b) => reqAcc.input(b.name, b.value),
      p.request()
    );

    const r = await reqMs.query(upd.sql);

    if (r.recordset.length === 0) {
      return res.status(404).json({ error: 'NOT_FOUND' });
    }
    res.json(r.recordset[0]);
  } catch (err) {
    console.error('PATCH /api/pacientes/:id:', err);
    if (err.message === 'EMPTY_BODY') {
      return res.status(400).json({ error: 'EMPTY_BODY', message: 'Nada que actualizar.' });
    }
    res.status(500).json({
      error: 'DB_ERROR', name: err.name, code: err.code, number: err.number, message: err.message
    });
  }
});

// DELETE /api/pacientes/:id  (borrado real)
app.delete('/api/pacientes/:id', async (req, res) => {
  try {
    const p = await getPool();
    const r = await p.request()
      .input('id', req.params.id)
      .query(`
        DELETE FROM ${PAC_TABLE}
        OUTPUT DELETED.*
        WHERE [${PAC_ID}] = @id;
      `);

    if (r.recordset.length === 0) {
      return res.status(404).json({ error: 'NOT_FOUND' });
    }
    res.json({ ok: true, deleted: r.recordset[0] });
  } catch (err) {
    console.error('DELETE /api/pacientes/:id:', err);
    res.status(500).json({
      error: 'DB_ERROR', name: err.name, code: err.code, number: err.number, message: err.message
    });
  }
});

// ===== Helpers por columna distinta a la PK =====
function buildUpdateWhere(table, whereCol, whereVal, body) {
  const keys = Object.keys(body).filter(k => k !== whereCol); // no permitir cambiar la cédula aquí
  if (keys.length === 0) throw new Error('EMPTY_BODY');
  const setClause = keys.map((k, i) => `[${k}] = @p${i}`).join(', ');
  return {
    sql: `
      UPDATE ${table}
      SET ${setClause}
      OUTPUT INSERTED.*
      WHERE [${whereCol}] = @whereVal;
    `,
    bind: [
      ...keys.map((k, i) => ({ name: `p${i}`, value: body[k] })),
      { name: 'whereVal', value: whereVal }
    ]
  };
}

// ===== GET por cédula =====
// GET /api/pacientes/cedula/:cedula
app.get('/api/pacientes/cedula/:cedula', async (req, res) => {
  try {
    const p = await getPool();
    const r = await p.request()
      .input('ced', req.params.cedula)
      .query(`
        SELECT TOP 1 *
        FROM ${PAC_TABLE}
        WHERE [identificacion_pac] = @ced;
      `);

    if (r.recordset.length === 0) {
      return res.status(404).json({ error: 'NOT_FOUND' });
    }
    res.json(r.recordset[0]);
  } catch (err) {
    console.error('GET /api/pacientes/cedula/:cedula:', err);
    res.status(500).json({
      error: 'DB_ERROR', name: err.name, code: err.code, number: err.number, message: err.message
    });
  }
});

// ===== PATCH por cédula =====
// PATCH /api/pacientes/cedula/:cedula
app.patch('/api/pacientes/cedula/:cedula', async (req, res) => {
  try {
    const p = await getPool();
    const body = { ...req.body };
    delete body.identificacion_pac; // evitamos cambiar la cédula en esta ruta

    const upd = buildUpdateWhere(PAC_TABLE, 'identificacion_pac', req.params.cedula, body);

    // Importante: no volver a inyectar 'whereVal' aparte; ya viene en bind
    const reqMs = upd.bind.reduce(
      (reqAcc, b) => reqAcc.input(b.name, b.value),
      p.request()
    );

    const r = await reqMs.query(upd.sql);
    if (r.recordset.length === 0) {
      return res.status(404).json({ error: 'NOT_FOUND' });
    }
    res.json(r.recordset[0]);
  } catch (err) {
    console.error('PATCH /api/pacientes/cedula/:cedula:', err);
    if (err.message === 'EMPTY_BODY') {
      return res.status(400).json({ error: 'EMPTY_BODY', message: 'Nada que actualizar.' });
    }
    res.status(500).json({
      error: 'DB_ERROR', name: err.name, code: err.code, number: err.number, message: err.message
    });
  }
});

// ===== DELETE por cédula =====
// DELETE /api/pacientes/cedula/:cedula
app.delete('/api/pacientes/cedula/:cedula', async (req, res) => {
  try {
    const p = await getPool();
    const r = await p.request()
      .input('ced', req.params.cedula)
      .query(`
        DELETE FROM ${PAC_TABLE}
        OUTPUT DELETED.*
        WHERE [identificacion_pac] = @ced;
      `);

    if (r.recordset.length === 0) {
      return res.status(404).json({ error: 'NOT_FOUND' });
    }
    res.json({ ok: true, deleted: r.recordset[0] });
  } catch (err) {
    console.error('DELETE /api/pacientes/cedula/:cedula:', err);
    res.status(500).json({
      error: 'DB_ERROR', name: err.name, code: err.code, number: err.number, message: err.message
    });
  }
});

// =========================
// Doctores CRUD (/api/doctores)
// =========================
const DOC_TABLE = '[MedPresc].dbo.[Doctor]';
const DOC_ID = 'codigo_doctor'; // PK

// POST /api/doctores  (crear)
app.post('/api/doctores', async (req, res) => {
  try {
    const p = await getPool();
    const body = { ...req.body }; // ej: num_licencia_med, cedula_doc, nombre_doc, ...

    const ins = buildInsert(DOC_TABLE, body);
    const r = await ins.bind.reduce((reqAcc, b) => reqAcc.input(b.name, b.value), p.request())
      .query(ins.sql);

    res.status(201).json(r.recordset[0]);
  } catch (err) {
    console.error('POST /api/doctores:', err);
    if (err.message === 'EMPTY_BODY') {
      return res.status(400).json({ error: 'EMPTY_BODY', message: 'Body vacío.' });
    }
    res.status(500).json({ error: 'DB_ERROR', name: err.name, code: err.code, number: err.number, message: err.message });
  }
});

// GET /api/doctores  (listar)  ?limit=50&offset=0
app.get('/api/doctores', async (req, res) => {
  try {
    const p = await getPool();
    const limit = Math.min(parseInt(req.query.limit || '50', 10), 200);
    const offset = parseInt(req.query.offset || '0', 10);

    const q = `
      SELECT *
      FROM ${DOC_TABLE}
      ORDER BY [${DOC_ID}] DESC
      OFFSET @offset ROWS FETCH NEXT @limit ROWS ONLY;
    `;
    const r = await p.request().input('offset', offset).input('limit', limit).query(q);
    res.json(r.recordset);
  } catch (err) {
    console.error('GET /api/doctores:', err);
    res.status(500).json({ error: 'DB_ERROR', name: err.name, code: err.code, number: err.number, message: err.message });
  }
});

// GET /api/doctores/:id  (detalle)
app.get('/api/doctores/:id', async (req, res) => {
  try {
    const p = await getPool();
    const r = await p.request()
      .input('id', req.params.id)
      .query(`
        SELECT TOP 1 *
        FROM ${DOC_TABLE}
        WHERE [${DOC_ID}] = @id;
      `);
    if (r.recordset.length === 0) return res.status(404).json({ error: 'NOT_FOUND' });
    res.json(r.recordset[0]);
  } catch (err) {
    console.error('GET /api/doctores/:id:', err);
    res.status(500).json({ error: 'DB_ERROR', name: err.name, code: err.code, number: err.number, message: err.message });
  }
});

// PATCH /api/doctores/:id  (actualización parcial por ID)
app.patch('/api/doctores/:id', async (req, res) => {
  try {
    const p = await getPool();
    const body = { ...req.body };
    delete body[DOC_ID]; // no modificar la PK

    const upd = buildUpdate(DOC_TABLE, DOC_ID, req.params.id, body);

    // ¡OJO! buildUpdate ya inyecta @id en bind; no lo pases de nuevo
    const reqMs = upd.bind.reduce((reqAcc, b) => reqAcc.input(b.name, b.value), p.request());
    const r = await reqMs.query(upd.sql);

    if (r.recordset.length === 0) return res.status(404).json({ error: 'NOT_FOUND' });
    res.json(r.recordset[0]);
  } catch (err) {
    console.error('PATCH /api/doctores/:id:', err);
    if (err.message === 'EMPTY_BODY') return res.status(400).json({ error: 'EMPTY_BODY', message: 'Nada que actualizar.' });
    res.status(500).json({ error: 'DB_ERROR', name: err.name, code: err.code, number: err.number, message: err.message });
  }
});

// DELETE /api/doctores/:id  (borrado real)
app.delete('/api/doctores/:id', async (req, res) => {
  try {
    const p = await getPool();
    const r = await p.request()
      .input('id', req.params.id)
      .query(`
        DELETE FROM ${DOC_TABLE}
        OUTPUT DELETED.*
        WHERE [${DOC_ID}] = @id;
      `);
    if (r.recordset.length === 0) return res.status(404).json({ error: 'NOT_FOUND' });
    res.json({ ok: true, deleted: r.recordset[0] });
  } catch (err) {
    console.error('DELETE /api/doctores/:id:', err);
    res.status(500).json({ error: 'DB_ERROR', name: err.name, code: err.code, number: err.number, message: err.message });
  }
});

// ===== Por cédula =====

// GET /api/doctores/cedula/:cedula
app.get('/api/doctores/cedula/:cedula', async (req, res) => {
  try {
    const p = await getPool();
    const r = await p.request()
      .input('ced', req.params.cedula)
      .query(`
        SELECT TOP 1 *
        FROM ${DOC_TABLE}
        WHERE [cedula_doc] = @ced;
      `);
    if (r.recordset.length === 0) return res.status(404).json({ error: 'NOT_FOUND' });
    res.json(r.recordset[0]);
  } catch (err) {
    console.error('GET /api/doctores/cedula/:cedula:', err);
    res.status(500).json({ error: 'DB_ERROR', name: err.name, code: err.code, number: err.number, message: err.message });
  }
});

// PATCH /api/doctores/cedula/:cedula  (no permite cambiar la cédula desde esta ruta)
app.patch('/api/doctores/cedula/:cedula', async (req, res) => {
  try {
    const p = await getPool();
    const body = { ...req.body };
    delete body.cedula_doc; // bloquear cambio de cédula aquí

    const upd = buildUpdateWhere(DOC_TABLE, 'cedula_doc', req.params.cedula, body);

    const reqMs = upd.bind.reduce((reqAcc, b) => reqAcc.input(b.name, b.value), p.request());
    const r = await reqMs.query(upd.sql);

    if (r.recordset.length === 0) return res.status(404).json({ error: 'NOT_FOUND' });
    res.json(r.recordset[0]);
  } catch (err) {
    console.error('PATCH /api/doctores/cedula/:cedula:', err);
    if (err.message === 'EMPTY_BODY') return res.status(400).json({ error: 'EMPTY_BODY', message: 'Nada que actualizar.' });
    res.status(500).json({ error: 'DB_ERROR', name: err.name, code: err.code, number: err.number, message: err.message });
  }
});

// DELETE /api/doctores/cedula/:cedula
app.delete('/api/doctores/cedula/:cedula', async (req, res) => {
  try {
    const p = await getPool();
    const r = await p.request()
      .input('ced', req.params.cedula)
      .query(`
        DELETE FROM ${DOC_TABLE}
        OUTPUT DELETED.*
        WHERE [cedula_doc] = @ced;
      `);
    if (r.recordset.length === 0) return res.status(404).json({ error: 'NOT_FOUND' });
    res.json({ ok: true, deleted: r.recordset[0] });
  } catch (err) {
    console.error('DELETE /api/doctores/cedula/:cedula:', err);
    res.status(500).json({ error: 'DB_ERROR', name: err.name, code: err.code, number: err.number, message: err.message });
  }
});


// =========================
// Farmacias (/api/farmacias)
// =========================
const FAR_TABLE = '[MedPresc].dbo.[Farmacia]';
const FAR_ID = 'codigo_farmacia';

// Escapar patrones para LIKE
function likeEscape(s) {
  return String(s || '').replace(/[\\%_\[]/g, c => '\\' + c);
}

// POST /api/farmacias
app.post('/api/farmacias', async (req, res) => {
  try {
    const p = await getPool();
    const body = { ...req.body };
    // created_at NOT NULL → si no viene, lo seteo
    if (!body.created_at) body.created_at = new Date();

    const ins = buildInsert(FAR_TABLE, body);
    const r = await ins.bind.reduce((acc, b) => acc.input(b.name, b.value), p.request())
      .query(ins.sql);
    res.status(201).json(r.recordset[0]);
  } catch (err) {
    console.error('POST /api/farmacias:', err);
    if (err.message === 'EMPTY_BODY') return res.status(400).json({ error: 'EMPTY_BODY', message: 'Body vacío.' });
    res.status(500).json({ error: 'DB_ERROR', name: err.name, code: err.code, number: err.number, message: err.message });
  }
});

// GET /api/farmacias  (lista) ?limit=&offset=
app.get('/api/farmacias', async (req, res) => {
  try {
    const p = await getPool();
    const limit = Math.min(parseInt(req.query.limit || '50', 10), 200);
    const offset = parseInt(req.query.offset || '0', 10);

    const r = await p.request()
      .input('offset', offset)
      .input('limit', limit)
      .query(`
        SELECT *
        FROM ${FAR_TABLE}
        ORDER BY [${FAR_ID}] DESC
        OFFSET @offset ROWS FETCH NEXT @limit ROWS ONLY;
      `);
    res.json(r.recordset);
  } catch (err) {
    console.error('GET /api/farmacias:', err);
    res.status(500).json({ error: 'DB_ERROR', name: err.name, code: err.code, number: err.number, message: err.message });
  }
});

// GET /api/farmacias/:id
app.get('/api/farmacias/:id', async (req, res) => {
  try {
    const p = await getPool();
    const r = await p.request().input('id', req.params.id).query(`
      SELECT TOP 1 * FROM ${FAR_TABLE} WHERE [${FAR_ID}] = @id;
    `);
    if (r.recordset.length === 0) return res.status(404).json({ error: 'NOT_FOUND' });
    res.json(r.recordset[0]);
  } catch (err) {
    console.error('GET /api/farmacias/:id:', err);
    res.status(500).json({ error: 'DB_ERROR', name: err.name, code: err.code, number: err.number, message: err.message });
  }
});

// PATCH /api/farmacias/:id
app.patch('/api/farmacias/:id', async (req, res) => {
  try {
    const p = await getPool();
    const body = { ...req.body };
    delete body[FAR_ID];
    delete body.created_at; // no permitimos editar creado

    const upd = buildUpdate(FAR_TABLE, FAR_ID, req.params.id, body);
    const reqMs = upd.bind.reduce((acc, b) => acc.input(b.name, b.value), p.request());
    const r = await reqMs.query(upd.sql);
    if (r.recordset.length === 0) return res.status(404).json({ error: 'NOT_FOUND' });
    res.json(r.recordset[0]);
  } catch (err) {
    console.error('PATCH /api/farmacias/:id:', err);
    if (err.message === 'EMPTY_BODY') return res.status(400).json({ error: 'EMPTY_BODY', message: 'Nada que actualizar.' });
    res.status(500).json({ error: 'DB_ERROR', name: err.name, code: err.code, number: err.number, message: err.message });
  }
});

// DELETE /api/farmacias/:id
app.delete('/api/farmacias/:id', async (req, res) => {
  try {
    const p = await getPool();
    const r = await p.request().input('id', req.params.id).query(`
      DELETE FROM ${FAR_TABLE}
      OUTPUT DELETED.*
      WHERE [${FAR_ID}] = @id;
    `);
    if (r.recordset.length === 0) return res.status(404).json({ error: 'NOT_FOUND' });
    res.json({ ok: true, deleted: r.recordset[0] });
  } catch (err) {
    console.error('DELETE /api/farmacias/:id:', err);
    res.status(500).json({ error: 'DB_ERROR', name: err.name, code: err.code, number: err.number, message: err.message });
  }
});

// GET /api/farmacias/contiene/:texto  (búsqueda parcial por nombre)
app.get('/api/farmacias/contiene/:texto', async (req, res) => {
  try {
    const p = await getPool();
    const limit = Math.min(parseInt(req.query.limit || '50', 10), 200);
    const offset = parseInt(req.query.offset || '0', 10);
    const pattern = `%${likeEscape(req.params.texto)}%`;

    const r = await p.request()
      .input('q', pattern)
      .input('offset', offset)
      .input('limit', limit)
      .query(`
        SELECT *
        FROM ${FAR_TABLE}
        WHERE nombre_farmacia COLLATE Latin1_General_CI_AI LIKE @q ESCAPE '\\'
        ORDER BY [${FAR_ID}] DESC
        OFFSET @offset ROWS FETCH NEXT @limit ROWS ONLY;
      `);
    res.json(r.recordset);
  } catch (err) {
    console.error('GET /api/farmacias/contiene/:texto:', err);
    res.status(500).json({ error: 'DB_ERROR', name: err.name, code: err.code, number: err.number, message: err.message });
  }
});

// GET /api/farmacias/nombre/:nombre  (coincidencia exacta)
app.get('/api/farmacias/nombre/:nombre', async (req, res) => {
  try {
    const p = await getPool();
    const r = await p.request().input('nombre', req.params.nombre).query(`
      SELECT * FROM ${FAR_TABLE}
      WHERE nombre_farmacia COLLATE Latin1_General_CI_AI = @nombre
      ORDER BY [${FAR_ID}] DESC;
    `);
    if (r.recordset.length === 0) return res.status(404).json({ error: 'NOT_FOUND' });
    res.json(r.recordset);
  } catch (err) {
    console.error('GET /api/farmacias/nombre/:nombre:', err);
    res.status(500).json({ error: 'DB_ERROR', name: err.name, code: err.code, number: err.number, message: err.message });
  }
});

// =========================
// Entidades Reguladoras (/api/reguladores)
// =========================
const REG_TABLE = '[MedPresc].dbo.[entidad_reguladora]';
const REG_ID = 'codigo_entidad_reg';

// POST /api/reguladores
app.post('/api/reguladores', async (req, res) => {
  try {
    const p = await getPool();
    const body = { ...req.body };
    if (!body.created_at) body.created_at = new Date(); // DATETIME2

    const ins = buildInsert(REG_TABLE, body);
    const r = await ins.bind.reduce((acc, b) => acc.input(b.name, b.value), p.request())
      .query(ins.sql);

    res.status(201).json(r.recordset[0]);
  } catch (err) {
    console.error('POST /api/reguladores:', err);
    if (err.message === 'EMPTY_BODY') return res.status(400).json({ error: 'EMPTY_BODY', message: 'Body vacío.' });
    res.status(500).json({ error: 'DB_ERROR', name: err.name, code: err.code, number: err.number, message: err.message });
  }
});

// GET /api/reguladores  (lista) ?limit=50&offset=0
app.get('/api/reguladores', async (req, res) => {
  try {
    const p = await getPool();
    const limit = Math.min(parseInt(req.query.limit || '50', 10), 200);
    const offset = parseInt(req.query.offset || '0', 10);

    const r = await p.request()
      .input('offset', offset)
      .input('limit', limit)
      .query(`
        SELECT *
        FROM ${REG_TABLE}
        ORDER BY [${REG_ID}] DESC
        OFFSET @offset ROWS FETCH NEXT @limit ROWS ONLY;
      `);

    res.json(r.recordset);
  } catch (err) {
    console.error('GET /api/reguladores:', err);
    res.status(500).json({ error: 'DB_ERROR', name: err.name, code: err.code, number: err.number, message: err.message });
  }
});

// GET /api/reguladores/:id  (detalle)
app.get('/api/reguladores/:id', async (req, res) => {
  try {
    const p = await getPool();
    const r = await p.request()
      .input('id', req.params.id)
      .query(`
        SELECT TOP 1 *
        FROM ${REG_TABLE}
        WHERE [${REG_ID}] = @id;
      `);

    if (r.recordset.length === 0) return res.status(404).json({ error: 'NOT_FOUND' });
    res.json(r.recordset[0]);
  } catch (err) {
    console.error('GET /api/reguladores/:id:', err);
    res.status(500).json({ error: 'DB_ERROR', name: err.name, code: err.code, number: err.number, message: err.message });
  }
});

// PATCH /api/reguladores/:id  (actualización parcial)
app.patch('/api/reguladores/:id', async (req, res) => {
  try {
    const p = await getPool();
    const body = { ...req.body };
    delete body[REG_ID];     // no tocar PK
    delete body.created_at;  // no editar fecha de creación

    const upd = buildUpdate(REG_TABLE, REG_ID, req.params.id, body);
    const reqMs = upd.bind.reduce((acc, b) => acc.input(b.name, b.value), p.request());
    const r = await reqMs.query(upd.sql);

    if (r.recordset.length === 0) return res.status(404).json({ error: 'NOT_FOUND' });
    res.json(r.recordset[0]);
  } catch (err) {
    console.error('PATCH /api/reguladores/:id:', err);
    if (err.message === 'EMPTY_BODY') return res.status(400).json({ error: 'EMPTY_BODY', message: 'Nada que actualizar.' });
    res.status(500).json({ error: 'DB_ERROR', name: err.name, code: err.code, number: err.number, message: err.message });
  }
});

// DELETE /api/reguladores/:id  (borrado real)
app.delete('/api/reguladores/:id', async (req, res) => {
  try {
    const p = await getPool();
    const r = await p.request()
      .input('id', req.params.id)
      .query(`
        DELETE FROM ${REG_TABLE}
        OUTPUT DELETED.*
        WHERE [${REG_ID}] = @id;
      `);

    if (r.recordset.length === 0) return res.status(404).json({ error: 'NOT_FOUND' });
    res.json({ ok: true, deleted: r.recordset[0] });
  } catch (err) {
    console.error('DELETE /api/reguladores/:id:', err);
    res.status(500).json({ error: 'DB_ERROR', name: err.name, code: err.code, number: err.number, message: err.message });
  }
});


// =========================
// Medicamentos (/api/medicamentos)
// =========================
const MED_TABLE = '[MedPresc].dbo.[Medicamento_API]';
const MED_ID = 'codigo_medicamento';

// util LIKE seguro (escape)
function likeEscape(s) {
  return String(s || '').replace(/[\\%_\[]/g, c => '\\' + c);
}

// fields=light|full
function selectFields(fields) {
  if ((fields || '').toLowerCase() === 'light') {
    return `[${MED_ID}], [DrugName], [Form], [Strength]`;
  }
  return `*`;
}

// ========== CREATE ==========
// POST /api/medicamentos
app.post('/api/medicamentos', async (req, res) => {
  try {
    const p = await getPool();
    const body = { ...req.body }; // envía solo columnas válidas de Medicamento_API

    const ins = buildInsert(MED_TABLE, body);
    const r = await ins.bind.reduce((acc, b) => acc.input(b.name, b.value), p.request())
      .query(ins.sql);
    res.status(201).json(r.recordset[0]);
  } catch (err) {
    console.error('POST /api/medicamentos:', err);
    if (err.message === 'EMPTY_BODY') return res.status(400).json({ error: 'EMPTY_BODY', message: 'Body vacío.' });
    res.status(500).json({ error: 'DB_ERROR', name: err.name, code: err.code, number: err.number, message: err.message });
  }
});

// ========== LIST (paginado + orden + fields) ==========
// GET /api/medicamentos?limit&offset&order=asc|desc&by=DrugName|Form|Strength&fields=light|full
app.get('/api/medicamentos', async (req, res) => {
  try {
    const p = await getPool();
    const limit  = Math.min(parseInt(req.query.limit  || '50', 10), 200);
    const offset = parseInt(req.query.offset || '0',  10);
    const order  = (req.query.order || 'desc').toLowerCase() === 'asc' ? 'ASC' : 'DESC';
    const byRaw  = (req.query.by || MED_ID);
    const allowedOrderBy = new Set([MED_ID, 'DrugName', 'Form', 'Strength']);
    const by = allowedOrderBy.has(byRaw) ? byRaw : MED_ID;
    const fields = selectFields(req.query.fields);

    const r = await p.request()
      .input('offset', offset)
      .input('limit', limit)
      .query(`
        SELECT ${fields}
        FROM ${MED_TABLE}
        ORDER BY [${by}] ${order}
        OFFSET @offset ROWS FETCH NEXT @limit ROWS ONLY;
      `);
    res.json(r.recordset);
  } catch (err) {
    console.error('GET /api/medicamentos:', err);
    res.status(500).json({ error: 'DB_ERROR', name: err.name, code: err.code, number: err.number, message: err.message });
  }
});

// ========== SEARCHES (poner ANTES de "/:id") ==========

// GET /api/medicamentos/contiene/:texto   (DrugName o ActiveIngredient)
app.get('/api/medicamentos/contiene/:texto', async (req, res) => {
  try {
    const p = await getPool();
    const limit  = Math.min(parseInt(req.query.limit  || '50', 10), 200);
    const offset = parseInt(req.query.offset || '0',  10);
    const fields = selectFields(req.query.fields);
    const q = `%${likeEscape(req.params.texto)}%`;

    const r = await p.request()
      .input('q', q)
      .input('offset', offset)
      .input('limit', limit)
      .query(`
        SELECT ${fields}
        FROM ${MED_TABLE}
        WHERE (DrugName COLLATE Latin1_General_CI_AI LIKE @q ESCAPE '\\'
           OR  ActiveIngredient COLLATE Latin1_General_CI_AI LIKE @q ESCAPE '\\')
        ORDER BY [${MED_ID}] DESC
        OFFSET @offset ROWS FETCH NEXT @limit ROWS ONLY;
      `);
    if (r.recordset.length === 0) return res.status(404).json({ error: 'NOT_FOUND' });
    res.json(r.recordset);
  } catch (err) {
    console.error('GET /api/medicamentos/contiene/:texto:', err);
    res.status(500).json({ error: 'DB_ERROR', name: err.name, code: err.code, number: err.number, message: err.message });
  }
});

// GET /api/medicamentos/nombre/:nombre (exacto por DrugName)
app.get('/api/medicamentos/nombre/:nombre', async (req, res) => {
  try {
    const p = await getPool();
    const fields = selectFields(req.query.fields);
    const r = await p.request()
      .input('nombre', req.params.nombre)
      .query(`
        SELECT ${fields}
        FROM ${MED_TABLE}
        WHERE DrugName COLLATE Latin1_General_CI_AI = @nombre
        ORDER BY [${MED_ID}] DESC;
      `);
    if (r.recordset.length === 0) return res.status(404).json({ error: 'NOT_FOUND' });
    res.json(r.recordset);
  } catch (err) {
    console.error('GET /api/medicamentos/nombre/:nombre:', err);
    res.status(500).json({ error: 'DB_ERROR', name: err.name, code: err.code, number: err.number, message: err.message });
  }
});

// GET /api/medicamentos/ingrediente/:texto  (parcial por ActiveIngredient)
app.get('/api/medicamentos/ingrediente/:texto', async (req, res) => {
  try {
    const p = await getPool();
    const limit  = Math.min(parseInt(req.query.limit  || '50', 10), 200);
    const offset = parseInt(req.query.offset || '0',  10);
    const fields = selectFields(req.query.fields);
    const q = `%${likeEscape(req.params.texto)}%`;

    const r = await p.request()
      .input('q', q)
      .input('offset', offset)
      .input('limit', limit)
      .query(`
        SELECT ${fields}
        FROM ${MED_TABLE}
        WHERE ActiveIngredient COLLATE Latin1_General_CI_AI LIKE @q ESCAPE '\\'
        ORDER BY [${MED_ID}] DESC
        OFFSET @offset ROWS FETCH NEXT @limit ROWS ONLY;
      `);
    if (r.recordset.length === 0) return res.status(404).json({ error: 'NOT_FOUND' });
    res.json(r.recordset);
  } catch (err) {
    console.error('GET /api/medicamentos/ingrediente/:texto:', err);
    res.status(500).json({ error: 'DB_ERROR', name: err.name, code: err.code, number: err.number, message: err.message });
  }
});

// FORM: exacto y parcial
app.get('/api/medicamentos/form/:form', async (req, res) => {
  try {
    const p = await getPool();
    const fields = selectFields(req.query.fields);
    const r = await p.request()
      .input('v', req.params.form)
      .query(`
        SELECT ${fields}
        FROM ${MED_TABLE}
        WHERE [Form] COLLATE Latin1_General_CI_AI = @v
        ORDER BY [${MED_ID}] DESC;
      `);
    if (r.recordset.length === 0) return res.status(404).json({ error: 'NOT_FOUND' });
    res.json(r.recordset);
  } catch (err) {
    console.error('GET /api/medicamentos/form/:form:', err);
    res.status(500).json({ error: 'DB_ERROR', name: err.name, code: err.code, number: err.number, message: err.message });
  }
});

app.get('/api/medicamentos/form/contiene/:texto', async (req, res) => {
  try {
    const p = await getPool();
    const fields = selectFields(req.query.fields);
    const q = `%${likeEscape(req.params.texto)}%`;
    const r = await p.request()
      .input('q', q)
      .query(`
        SELECT ${fields}
        FROM ${MED_TABLE}
        WHERE [Form] COLLATE Latin1_General_CI_AI LIKE @q ESCAPE '\\'
        ORDER BY [${MED_ID}] DESC;
      `);
    if (r.recordset.length === 0) return res.status(404).json({ error: 'NOT_FOUND' });
    res.json(r.recordset);
  } catch (err) {
    console.error('GET /api/medicamentos/form/contiene/:texto:', err);
    res.status(500).json({ error: 'DB_ERROR', name: err.name, code: err.code, number: err.number, message: err.message });
  }
});

// STRENGTH: exacto y parcial
app.get('/api/medicamentos/strength/:strength', async (req, res) => {
  try {
    const p = await getPool();
    const fields = selectFields(req.query.fields);
    const r = await p.request()
      .input('v', req.params.strength)
      .query(`
        SELECT ${fields}
        FROM ${MED_TABLE}
        WHERE [Strength] COLLATE Latin1_General_CI_AI = @v
        ORDER BY [${MED_ID}] DESC;
      `);
    if (r.recordset.length === 0) return res.status(404).json({ error: 'NOT_FOUND' });
    res.json(r.recordset);
  } catch (err) {
    console.error('GET /api/medicamentos/strength/:strength:', err);
    res.status(500).json({ error: 'DB_ERROR', name: err.name, code: err.code, number: err.number, message: err.message });
  }
});

app.get('/api/medicamentos/strength/contiene/:texto', async (req, res) => {
  try {
    const p = await getPool();
    const fields = selectFields(req.query.fields);
    const q = `%${likeEscape(req.params.texto)}%`;
    const r = await p.request()
      .input('q', q)
      .query(`
        SELECT ${fields}
        FROM ${MED_TABLE}
        WHERE [Strength] COLLATE Latin1_General_CI_AI LIKE @q ESCAPE '\\'
        ORDER BY [${MED_ID}] DESC;
      `);
    if (r.recordset.length === 0) return res.status(404).json({ error: 'NOT_FOUND' });
    res.json(r.recordset);
  } catch (err) {
    console.error('GET /api/medicamentos/strength/contiene/:texto:', err);
    res.status(500).json({ error: 'DB_ERROR', name: err.name, code: err.code, number: err.number, message: err.message });
  }
});

// EXISTS (duplicados)
// GET /api/medicamentos/exists?drugName=...&form=...&strength=...
app.get('/api/medicamentos/exists', async (req, res) => {
  try {
    const dn = (req.query.drugName || '').trim();
    const fm = (req.query.form || '').trim() || null;
    const st = (req.query.strength || '').trim() || null;
    if (!dn) return res.status(400).json({ error: 'MISSING_PARAM', message: 'Requiere drugName' });

    const p = await getPool();
    const r = await p.request()
      .input('dn', dn)
      .input('fm', fm)
      .input('st', st)
      .query(`
        SELECT CASE WHEN EXISTS (
          SELECT 1 FROM ${MED_TABLE}
          WHERE DrugName COLLATE Latin1_General_CI_AI = @dn
            AND (@fm IS NULL OR [Form] COLLATE Latin1_General_CI_AI = @fm)
            AND (@st IS NULL OR [Strength] COLLATE Latin1_General_CI_AI = @st)
        ) THEN CAST(1 AS bit) ELSE CAST(0 AS bit) END AS ex;
      `);
    res.json({ exists: Boolean(r.recordset[0].ex) });
  } catch (err) {
    console.error('GET /api/medicamentos/exists:', err);
    res.status(500).json({ error: 'DB_ERROR', name: err.name, code: err.code, number: err.number, message: err.message });
  }
});

// ========== DETAIL ==========
// (dejar al FINAL para no colisionar con rutas anteriores)
app.get('/api/medicamentos/:id', async (req, res) => {
  try {
    const p = await getPool();
    const r = await p.request().input('id', req.params.id).query(`
      SELECT * FROM ${MED_TABLE} WHERE [${MED_ID}] = @id;
    `);
    if (r.recordset.length === 0) return res.status(404).json({ error: 'NOT_FOUND' });
    res.json(r.recordset[0]);
  } catch (err) {
    console.error('GET /api/medicamentos/:id:', err);
    res.status(500).json({ error: 'DB_ERROR', name: err.name, code: err.code, number: err.number, message: err.message });
  }
});

// ========== UPDATE ==========
app.patch('/api/medicamentos/:id', async (req, res) => {
  try {
    const p = await getPool();
    const body = { ...req.body };
    delete body[MED_ID];

    const upd = buildUpdate(MED_TABLE, MED_ID, req.params.id, body);
    const reqMs = upd.bind.reduce((acc, b) => acc.input(b.name, b.value), p.request());
    const r = await reqMs.query(upd.sql);
    if (r.recordset.length === 0) return res.status(404).json({ error: 'NOT_FOUND' });
    res.json(r.recordset[0]);
  } catch (err) {
    console.error('PATCH /api/medicamentos/:id:', err);
    if (err.message === 'EMPTY_BODY') return res.status(400).json({ error: 'EMPTY_BODY', message: 'Nada que actualizar.' });
    res.status(500).json({ error: 'DB_ERROR', name: err.name, code: err.code, number: err.number, message: err.message });
  }
});

// ========== DELETE ==========
app.delete('/api/medicamentos/:id', async (req, res) => {
  try {
    const p = await getPool();
    const r = await p.request().input('id', req.params.id).query(`
      DELETE FROM ${MED_TABLE}
      OUTPUT DELETED.*
      WHERE [${MED_ID}] = @id;
    `);
    if (r.recordset.length === 0) return res.status(404).json({ error: 'NOT_FOUND' });
    res.json({ ok: true, deleted: r.recordset[0] });
  } catch (err) {
    console.error('DELETE /api/medicamentos/:id:', err);
    res.status(500).json({ error: 'DB_ERROR', name: err.name, code: err.code, number: err.number, message: err.message });
  }
});

// =========================
// Recetas - PARTE 1
// =========================
const REC_TABLE = '[MedPresc].dbo.[Receta]';
const PAC_TABLE = '[MedPresc].dbo.[Paciente]';
const DOC_TABLE = '[MedPresc].dbo.[Doctor]';
const RXITEM_TABLE = '[MedPresc].dbo.[medicamento_por_receta]';

// Estados válidos según tu CHECK: ISSUED | DISPENSED | REVOKED
const ESTADOS_DB = new Set(['ISSUED','DISPENSED','REVOKED']);
const ESTADO_MAP = {
  'emitida':'ISSUED','issued':'ISSUED',
  'dispensada':'DISPENSED','dispensed':'DISPENSED',
  'revocada':'REVOKED','revoked':'REVOKED'
};
function normalizeEstado(v){
  if (v == null) return null;
  const s = String(v).trim();
  const mapped = ESTADO_MAP[s.toLowerCase()] || s;
  return mapped.toUpperCase();
}

// Util LIKE escape
function likeEscape(s){ return String(s||'').replace(/[\\%_\[]/g, c => '\\'+c); }

// === helper: trae items de la receta (si existen columnas mínimas) ===
async function fetchRecetaItems(p, codigoReceta){
  try{
    const q = await p.request().input('c', codigoReceta).query(`
      SELECT * FROM ${RXITEM_TABLE} WHERE codigo_receta = @c ORDER BY 1;
    `);
    return q.recordset || [];
  }catch{
    return []; // si no existe la tabla/permiso, devolvemos vacío
  }
}

// 1) GET /api/recetas/codigo/:codigo
app.get('/api/recetas/codigo/:codigo', async (req,res)=>{
  try{
    const p = await getPool();
    const r = await p.request().input('codigo', req.params.codigo).query(`
      SELECT r.*, p.nombre_pac, p.apellido_pac, d.nombre_doc, d.apellido_doc
      FROM ${REC_TABLE} r
      LEFT JOIN ${PAC_TABLE} p ON p.id_paciente = r.id_paciente
      LEFT JOIN ${DOC_TABLE} d ON d.codigo_doctor = r.codigo_doctor
      WHERE r.codigo_receta = @codigo;
    `);
    if (!r.recordset.length) return res.status(404).json({ error:'NOT_FOUND' });
    res.json(r.recordset[0]);
  }catch(err){
    console.error('GET /recetas/codigo/:codigo', err);
    res.status(500).json({ error:'DB_ERROR', message: err.message });
  }
});

// 2) GET /api/recetas/hash/:rx_hash
app.get('/api/recetas/hash/:rx_hash', async (req,res)=>{
  try{
    const p = await getPool();
    const r = await p.request().input('h', req.params.rx_hash).query(`
      SELECT r.*, p.nombre_pac, p.apellido_pac, d.nombre_doc, d.apellido_doc
      FROM ${REC_TABLE} r
      LEFT JOIN ${PAC_TABLE} p ON p.id_paciente = r.id_paciente
      LEFT JOIN ${DOC_TABLE} d ON d.codigo_doctor = r.codigo_doctor
      WHERE r.rx_hash = @h;
    `);
    if (!r.recordset.length) return res.status(404).json({ error:'NOT_FOUND' });
    res.json(r.recordset[0]);
  }catch(err){
    console.error('GET /recetas/hash/:rx_hash', err);
    res.status(500).json({ error:'DB_ERROR', message: err.message });
  }
});

// 3) GET /api/recetas/qr/:jti  (validación rápida del QR)
app.get('/api/recetas/qr/:jti', async (req,res)=>{
  try{
    const p = await getPool();
    const r = await p.request().input('jti', req.params.jti).query(`
      SELECT r.*, p.nombre_pac, p.apellido_pac, d.nombre_doc, d.apellido_doc
      FROM ${REC_TABLE} r
      LEFT JOIN ${PAC_TABLE} p ON p.id_paciente = r.id_paciente
      LEFT JOIN ${DOC_TABLE} d ON d.codigo_doctor = r.codigo_doctor
      WHERE r.qr_jti = @jti;
    `);
    if (!r.recordset.length) return res.status(404).json({ error:'NOT_FOUND' });

    const x = r.recordset[0];
    const now = new Date();
    const exp = x.qr_exp ? new Date(x.qr_exp) : null;
    const razones = [];
    if (!exp) razones.push('SIN_EXPIRACION');
    if (exp && exp <= now) razones.push('QR_VENCIDO');
    if (x.qr_used) razones.push('QR_USADO');
    if (String(x.estado_receta).toUpperCase()==='REVOKED') razones.push('REVOKED');
    const valida = razones.length === 0;

    const items = await fetchRecetaItems(p, x.codigo_receta);

    res.json({
      valida,
      razones,
      expira_en_ms: exp ? Math.max(0, exp - now) : null,
      usada: !!x.qr_used,
      estado: x.estado_receta,
      receta: {
        codigo_receta: x.codigo_receta,
        fecha_receta: x.fecha_receta,
        qr_jti: x.qr_jti,
        qr_exp: x.qr_exp
      },
      paciente: x.id_paciente ? { id: x.id_paciente, nombre: x.nombre_pac, apellido: x.apellido_pac } : null,
      doctor:   x.codigo_doctor ? { id: x.codigo_doctor, nombre: x.nombre_doc, apellido: x.apellido_doc } : null,
      items
    });
  }catch(err){
    console.error('GET /recetas/qr/:jti', err);
    res.status(500).json({ error:'DB_ERROR', message: err.message });
  }
});

// 4) PUT /api/recetas/:id/dispensar
// Body: { onchain_tx_dispense: "0x...", qr_scanned_by_pharmacy_id: 123 }
app.put('/api/recetas/:id/dispensar', async (req,res)=>{
  try{
    const { onchain_tx_dispense = null, qr_scanned_by_pharmacy_id = null } = req.body || {};
    const p = await getPool();

    const u = await p.request()
      .input('id', req.params.id)
      .input('tx', onchain_tx_dispense)
      .input('ph', qr_scanned_by_pharmacy_id)
      .query(`
        UPDATE ${REC_TABLE}
        SET estado_receta = 'DISPENSED',
            onchain_tx_dispense = @tx,
            qr_used = 1,
            qr_used_ts = SYSUTCDATETIME(),
            qr_scanned_by_pharmacy_id = @ph
        OUTPUT INSERTED.*
        WHERE codigo_receta = @id
          AND UPPER(estado_receta) = 'ISSUED'
          AND (qr_used = 0 OR qr_used IS NULL);
      `);

    if (!u.recordset.length){
      const ex = await p.request().input('id', req.params.id)
        .query(`SELECT estado_receta, qr_used FROM ${REC_TABLE} WHERE codigo_receta = @id;`);
      if (!ex.recordset.length) return res.status(404).json({ error:'NOT_FOUND' });
      return res.status(409).json({ error:'NO_APLICA', message:'No está ISSUED o ya fue usada.' });
    }
    res.json(u.recordset[0]);
  }catch(err){
    console.error('PUT /recetas/:id/dispensar', err);
    res.status(500).json({ error:'DB_ERROR', message: err.message });
  }
});

// 5) GET /api/pacientes/:id/recetas  (paginado + filtros)
app.get('/api/pacientes/:id/recetas', async (req,res)=>{
  try{
    const p = await getPool();
    const limit  = Math.min(parseInt(req.query.limit || '50',10), 200);
    const offset = parseInt(req.query.offset || '0',10);
    const estado = normalizeEstado(req.query.estado) || null;
    if (req.query.estado && !ESTADOS_DB.has(estado))
      return res.status(400).json({ error:'INVALID_ESTADO', allowed: [...ESTADOS_DB] });

    const desde  = (req.query.desde || '').trim() || null;
    const hasta  = (req.query.hasta || '').trim() || null;

    const r = await p.request()
      .input('id', req.params.id)
      .input('estado', estado)
      .input('desde',  desde)
      .input('hasta',  hasta)
      .input('offset', offset)
      .input('limit',  limit)
      .query(`
        SELECT *
        FROM ${REC_TABLE}
        WHERE id_paciente = @id
          AND (@estado IS NULL OR estado_receta = @estado)
          AND (@desde IS NULL OR fecha_receta >= @desde)
          AND (@hasta IS NULL OR fecha_receta < DATEADD(day, 1, @hasta))
        ORDER BY fecha_receta DESC
        OFFSET @offset ROWS FETCH NEXT @limit ROWS ONLY;
      `);
    res.json(r.recordset);
  }catch(err){
    console.error('GET /pacientes/:id/recetas', err);
    res.status(500).json({ error:'DB_ERROR', message: err.message });
  }
});

// 6) GET /api/doctores/:id/recetas  (paginado + filtros)
app.get('/api/doctores/:id/recetas', async (req,res)=>{
  try{
    const p = await getPool();
    const limit  = Math.min(parseInt(req.query.limit || '50',10), 200);
    const offset = parseInt(req.query.offset || '0',10);
    const estado = normalizeEstado(req.query.estado) || null;
    if (req.query.estado && !ESTADOS_DB.has(estado))
      return res.status(400).json({ error:'INVALID_ESTADO', allowed: [...ESTADOS_DB] });

    const desde  = (req.query.desde || '').trim() || null;
    const hasta  = (req.query.hasta || '').trim() || null;

    const r = await p.request()
      .input('id', req.params.id)
      .input('estado', estado)
      .input('desde',  desde)
      .input('hasta',  hasta)
      .input('offset', offset)
      .input('limit',  limit)
      .query(`
        SELECT *
        FROM ${REC_TABLE}
        WHERE codigo_doctor = @id
          AND (@estado IS NULL OR estado_receta = @estado)
          AND (@desde IS NULL OR fecha_receta >= @desde)
          AND (@hasta IS NULL OR fecha_receta < DATEADD(day, 1, @hasta))
        ORDER BY fecha_receta DESC
        OFFSET @offset ROWS FETCH NEXT @limit ROWS ONLY;
      `);
    res.json(r.recordset);
  }catch(err){
    console.error('GET /doctores/:id/recetas', err);
    res.status(500).json({ error:'DB_ERROR', message: err.message });
  }
});

// 7) PUT /api/recetas/:id/estado  (ISSUED|DISPENSED|REVOKED)
app.put('/api/recetas/:id/estado', async (req,res)=>{
  try{
    const next = normalizeEstado(req.body?.estado);
    if (!ESTADOS_DB.has(next))
      return res.status(400).json({ error:'INVALID_ESTADO', allowed: [...ESTADOS_DB] });

    const p = await getPool();
    const r = await p.request()
      .input('id', req.params.id)
      .input('e', next)
      .query(`
        UPDATE ${REC_TABLE}
        SET estado_receta = @e
        OUTPUT INSERTED.*
        WHERE codigo_receta = @id;
      `);
    if (!r.recordset.length) return res.status(404).json({ error:'NOT_FOUND' });
    res.json(r.recordset[0]);
  }catch(err){
    console.error('PUT /recetas/:id/estado', err);
    res.status(500).json({ error:'DB_ERROR', message: err.message });
  }
});

// 8) POST /api/recetas/:id/qr/emitir  (solo si no tiene o override)
// Body opcional: { ttl_hours: 168, override_if_exists: false }
app.post('/api/recetas/:id/qr/emitir', async (req,res)=>{
  try{
    const ttl = Number(req.body?.ttl_hours ?? 168);
    const override = !!req.body?.override_if_exists;
    const p = await getPool();

    const r = await p.request()
      .input('id', req.params.id)
      .input('ttl', isNaN(ttl) ? 168 : ttl)
      .input('ovr', override ? 1 : 0)
      .query(`
        UPDATE ${REC_TABLE}
        SET qr_jti = LOWER(CONVERT(varchar(36), NEWID())),
            qr_exp = DATEADD(hour, @ttl, SYSUTCDATETIME()),
            qr_used = 0,
            qr_used_ts = NULL
        OUTPUT INSERTED.codigo_receta, INSERTED.qr_jti, INSERTED.qr_exp
        WHERE codigo_receta = @id
          AND (qr_jti IS NULL OR @ovr = 1);
      `);

    if (!r.recordset.length)
      return res.status(409).json({ error:'EXISTS', message:'Ya tiene QR. Envía override_if_exists=true para reemitir.' });

    res.status(201).json(r.recordset[0]);
  }catch(err){
    console.error('POST /recetas/:id/qr/emitir', err);
    res.status(500).json({ error:'DB_ERROR', message: err.message });
  }
});

// 9) POST /api/recetas/:id/qr/reemitir  (siempre genera uno nuevo)
// Body opcional: { ttl_hours: 168 }
app.post('/api/recetas/:id/qr/reemitir', async (req,res)=>{
  try{
    const ttl = Number(req.body?.ttl_hours ?? 168);
    const p = await getPool();

    const r = await p.request()
      .input('id', req.params.id)
      .input('ttl', isNaN(ttl) ? 168 : ttl)
      .query(`
        UPDATE ${REC_TABLE}
        SET qr_jti  = LOWER(CONVERT(varchar(36), NEWID())),
            qr_exp  = DATEADD(hour, @ttl, SYSUTCDATETIME()),
            qr_used = 0,
            qr_used_ts = NULL
        OUTPUT INSERTED.codigo_receta, INSERTED.qr_jti, INSERTED.qr_exp
        WHERE codigo_receta = @id;
      `);
    if (!r.recordset.length) return res.status(404).json({ error:'NOT_FOUND' });
    res.status(201).json(r.recordset[0]);
  }catch(err){
    console.error('POST /recetas/:id/qr/reemitir', err);
    res.status(500).json({ error:'DB_ERROR', message: err.message });
  }
});

// 10) PUT /api/recetas/:id/revocar  (bloquea si DISPENSED)
// Body opcional: { onchain_tx_revoke: "0x..." }
app.put('/api/recetas/:id/revocar', async (req,res)=>{
  try{
    const { onchain_tx_revoke = null } = req.body || {};
    const p = await getPool();
    const r = await p.request()
      .input('id', req.params.id)
      .input('tx', onchain_tx_revoke)
      .query(`
        UPDATE ${REC_TABLE}
        SET estado_receta = 'REVOKED',
            onchain_tx_revoke = @tx
        OUTPUT INSERTED.*
        WHERE codigo_receta = @id
          AND UPPER(estado_receta) <> 'DISPENSED';
      `);
    if (!r.recordset.length)
      return res.status(409).json({ error:'NO_APLICA', message:'No existe o ya fue DISPENSED.' });
    res.json(r.recordset[0]);
  }catch(err){
    console.error('PUT /recetas/:id/revocar', err);
    res.status(500).json({ error:'DB_ERROR', message: err.message });
  }
});


const port = process.env.PORT || 3000;
app.listen(port, () => console.log(`Listening on port ${port}...`));