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
// Recetas - PARTE 1 (incluye POST crear)
// =========================
const REC_TABLE     = '[MedPresc].dbo.[Receta]';
const PAC_TABLE_R   = '[MedPresc].dbo.[Paciente]';   // nombres del paciente
const DOC_TABLE_R   = '[MedPresc].dbo.[Doctor]';     // nombres del doctor
const RXITEM_TABLE  = '[MedPresc].dbo.[medicamento_por_receta]';

// Estados válidos según CHECK (ISSUED|DISPENSED|REVOKED)
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

const crypto = require('crypto');
function genCodigoReceta(){
  const d = new Date();
  const y = d.getUTCFullYear();
  const m = String(d.getUTCMonth()+1).padStart(2,'0');
  const day = String(d.getUTCDate()).padStart(2,'0');
  const rnd = crypto.randomBytes(3).toString('hex').toUpperCase(); // 6 hex
  return `RX-${y}${m}${day}-${rnd}`;
}
function sha256Hex(s){ return crypto.createHash('sha256').update(s,'utf8').digest('hex'); }

// Traer items (para /qr/:jti)
async function fetchRecetaItems(p, codigoReceta){
  try{
    const q = await p.request().input('c', codigoReceta).query(`
      SELECT codigo_medicamento, codigo_receta, nombre_medicamento, dosis_medicamento
      FROM ${RXITEM_TABLE}
      WHERE codigo_receta = @c
      ORDER BY 1;
    `);
    return q.recordset || [];
  }catch{ return []; }
}

/* =====================================================
 * 0) POST /api/recetas  (crear receta + items opcional)
 * Body mínimo: { id_paciente, codigo_doctor, created_by }
 * Opcional: diagnostico, estado_receta, onchain_tx_create, chain_id,
 *           channel_name, contract_name, qr_emitir, qr_ttl_hours,
 *           items: [{ codigo_medicamento, nombre_medicamento?, dosis_medicamento? }]
 * ===================================================== */
app.post('/api/recetas', async (req, res) => {
  const body = req.body || {};
  try{
    if (!body.id_paciente || !body.codigo_doctor || body.created_by == null){
      return res.status(400).json({
        error:'MISSING_FIELDS',
        message:'id_paciente, codigo_doctor y created_by son requeridos'
      });
    }

    const p = await getPool();
    const tx = new sql.Transaction(p);
    await tx.begin();

    try{
      const codigo_receta = body.codigo_receta || genCodigoReceta();
      const fecha_receta  = body.fecha_receta ? new Date(body.fecha_receta) : new Date();
      const estado_receta = normalizeEstado(body.estado_receta) || 'ISSUED';
      if (!ESTADOS_DB.has(estado_receta)) throw new Error(`INVALID_ESTADO:${estado_receta}`);

      // QR opcional
      const qr_emitir = !!body.qr_emitir;
      const ttlHours  = Number(body.qr_ttl_hours ?? 168);
      const qr_jti    = qr_emitir ? (crypto.randomUUID ? crypto.randomUUID() : String(crypto.randomBytes(16).toString('hex'))) : null;
      const qr_exp    = qr_emitir ? new Date(Date.now() + (isNaN(ttlHours)?168:ttlHours)*3600*1000) : null;

      // Items limitados a columnas reales de medicamento_por_receta
      const itemsLight = Array.isArray(body.items) ? body.items.map(it => ({
        codigo_medicamento: it.codigo_medicamento ?? it.id_medicamento ?? null,
        nombre_medicamento: it.nombre_medicamento ?? null,
        dosis_medicamento:  it.dosis_medicamento  ?? null
      })) : [];

      // Hash inmutable (solo con campos relevantes)
      const hashPayload = {
        codigo_receta,
        id_paciente: body.id_paciente,
        codigo_doctor: body.codigo_doctor,
        fecha_receta,
        diagnostico: body.diagnostico || null,
        items: itemsLight
      };
      const rx_hash = sha256Hex(JSON.stringify(hashPayload));

      // INSERT Receta
      const reqRec = new sql.Request(tx);
      reqRec
        .input('codigo_receta', sql.VarChar(40), codigo_receta)
        .input('id_paciente', sql.Int, body.id_paciente)
        .input('codigo_doctor', sql.Int, body.codigo_doctor)
        .input('diagnostico', sql.NVarChar(500), body.diagnostico || null)
        .input('fecha_receta', sql.DateTime2, fecha_receta)
        .input('estado_receta', sql.VarChar(12), estado_receta)
        .input('rx_hash', sql.Char(64), rx_hash)
        .input('onchain_tx_create', sql.VarChar(128), body.onchain_tx_create || null)
        .input('chain_id', sql.VarChar(32), body.chain_id || null)
        .input('channel_name', sql.VarChar(64), body.channel_name || null)
        .input('contract_name', sql.VarChar(64), body.contract_name || null)
        .input('qr_jti', sql.Char(36), qr_jti)
        .input('qr_exp', sql.DateTime2, qr_exp)
        .input('qr_used', sql.Bit, 0)
        .input('qr_used_ts', sql.DateTime2, null)
        .input('qr_scanned_by_pharmacy_id', sql.Int, null)
        .input('created_at', sql.DateTime2, new Date())
        .input('created_by', sql.Int, body.created_by);

      const insRec = await reqRec.query(`
        INSERT INTO ${REC_TABLE}
        (codigo_receta, id_paciente, codigo_doctor, diagnostico, fecha_receta, estado_receta,
         rx_hash, onchain_tx_create, chain_id, channel_name, contract_name,
         qr_jti, qr_exp, qr_used, qr_used_ts, qr_scanned_by_pharmacy_id,
         created_at, created_by)
        OUTPUT INSERTED.*
        VALUES
        (@codigo_receta, @id_paciente, @codigo_doctor, @diagnostico, @fecha_receta, @estado_receta,
         @rx_hash, @onchain_tx_create, @chain_id, @channel_name, @contract_name,
         @qr_jti, @qr_exp, @qr_used, @qr_used_ts, @qr_scanned_by_pharmacy_id,
         @created_at, @created_by);
      `);

      const receta = insRec.recordset[0];

      // INSERT de items
      let itemsInsertados = [];
      if (itemsLight.length){
        for (const it of itemsLight){
          if (it.codigo_medicamento == null) continue; // necesario
          const reqIt = new sql.Request(tx);
          reqIt
            .input('codMed', sql.Int, it.codigo_medicamento)
            .input('codRec', sql.VarChar(40), codigo_receta)
            .input('nom',    sql.NVarChar(200), it.nombre_medicamento ?? null)
            .input('dosis',  sql.NVarChar(100), it.dosis_medicamento  ?? null);
          const insIt = await reqIt.query(`
            INSERT INTO ${RXITEM_TABLE}
              (codigo_medicamento, codigo_receta, nombre_medicamento, dosis_medicamento)
            OUTPUT INSERTED.*
            VALUES (@codMed, @codRec, @nom, @dosis);
          `);
          itemsInsertados.push(insIt.recordset[0]);
        }
      }

      await tx.commit();
      res.status(201).json({ ...receta, items: itemsInsertados });
    }catch(e){
      await tx.rollback();
      if (String(e.message).startsWith('INVALID_ESTADO'))
        return res.status(400).json({ error:'INVALID_ESTADO', allowed:[...ESTADOS_DB] });
      throw e;
    }
  }catch(err){
    console.error('POST /recetas', err);
    res.status(500).json({ error:'DB_ERROR', message: err.message });
  }
});

/* =====================================================
 * 1) GET /api/recetas/codigo/:codigo
 * ===================================================== */
app.get('/api/recetas/codigo/:codigo', async (req,res)=>{
  try{
    const p = await getPool();
    const r = await p.request().input('codigo', req.params.codigo).query(`
      SELECT r.*, p.nombre_pac, p.apellido_pac, d.nombre_doc, d.apellido_doc
      FROM ${REC_TABLE} r
      LEFT JOIN ${PAC_TABLE_R} p ON p.id_paciente = r.id_paciente
      LEFT JOIN ${DOC_TABLE_R} d ON d.codigo_doctor = r.codigo_doctor
      WHERE r.codigo_receta = @codigo;
    `);
    if (!r.recordset.length) return res.status(404).json({ error:'NOT_FOUND' });
    res.json(r.recordset[0]);
  }catch(err){
    console.error('GET /recetas/codigo/:codigo', err);
    res.status(500).json({ error:'DB_ERROR', message: err.message });
  }
});

/* =====================================================
 * 2) GET /api/recetas/hash/:rx_hash
 * ===================================================== */
app.get('/api/recetas/hash/:rx_hash', async (req,res)=>{
  try{
    const p = await getPool();
    const r = await p.request().input('h', req.params.rx_hash).query(`
      SELECT r.*, p.nombre_pac, p.apellido_pac, d.nombre_doc, d.apellido_doc
      FROM ${REC_TABLE} r
      LEFT JOIN ${PAC_TABLE_R} p ON p.id_paciente = r.id_paciente
      LEFT JOIN ${DOC_TABLE_R} d ON d.codigo_doctor = r.codigo_doctor
      WHERE r.rx_hash = @h;
    `);
    if (!r.recordset.length) return res.status(404).json({ error:'NOT_FOUND' });
    res.json(r.recordset[0]);
  }catch(err){
    console.error('GET /recetas/hash/:rx_hash', err);
    res.status(500).json({ error:'DB_ERROR', message: err.message });
  }
});

/* =====================================================
 * 3) GET /api/recetas/qr/:jti  (validación rápida del QR)
 * ===================================================== */
app.get('/api/recetas/qr/:jti', async (req,res)=>{
  try{
    const p = await getPool();
    const r = await p.request().input('jti', req.params.jti).query(`
      SELECT r.*, p.nombre_pac, p.apellido_pac, d.nombre_doc, d.apellido_doc
      FROM ${REC_TABLE} r
      LEFT JOIN ${PAC_TABLE_R} p ON p.id_paciente = r.id_paciente
      LEFT JOIN ${DOC_TABLE_R} d ON d.codigo_doctor = r.codigo_doctor
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

/* =====================================================
 * 4) PUT /api/recetas/:id/dispensar
 * Body: { onchain_tx_dispense: "0x...", qr_scanned_by_pharmacy_id: 123 }
 * ===================================================== */
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

/* =====================================================
 * 5) GET /api/pacientes/:id/recetas  (paginado + filtros)
 * ===================================================== */
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

/* =====================================================
 * 6) GET /api/doctores/:id/recetas  (paginado + filtros)
 * ===================================================== */
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

/* =====================================================
 * 7) PUT /api/recetas/:id/estado  (ISSUED|DISPENSED|REVOKED)
 * ===================================================== */
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

/* =====================================================
 * 8) POST /api/recetas/:id/qr/emitir  (si no tiene QR o override)
 * Body opcional: { ttl_hours: 168, override_if_exists: false }
 * ===================================================== */
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

/* =====================================================
 * 9) POST /api/recetas/:id/qr/reemitir  (siempre genera uno nuevo)
 * Body opcional: { ttl_hours: 168 }
 * ===================================================== */
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

/* =====================================================
 * 10) PUT /api/recetas/:id/revocar  (bloquea si DISPENSED)
 * Body opcional: { onchain_tx_revoke: "0x..." }
 * ===================================================== */
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

// =========================
// Recetas - Filtros & Listados + Consulta puntual
// =========================

// GET /api/recetas
// Listado general con filtros: ?estado&paciente&doctor&desde&hasta&campo=fecha_receta|created_at&order=asc|desc&limit&offset
app.get('/api/recetas', async (req, res) => {
  try {
    const p = await getPool();
    const limit  = Math.min(parseInt(req.query.limit  || '50', 10), 200);
    const offset = parseInt(req.query.offset || '0', 10);
    const estado = normalizeEstado(req.query.estado) || null;
    if (req.query.estado && !ESTADOS_DB.has(estado)) {
      return res.status(400).json({ error: 'INVALID_ESTADO', allowed: [...ESTADOS_DB] });
    }
    const paciente = req.query.paciente ? parseInt(req.query.paciente, 10) : null;
    const doctor   = req.query.doctor   ? parseInt(req.query.doctor,   10) : null;

    const desde = (req.query.desde || '').trim() || null;
    const hasta = (req.query.hasta || '').trim() || null;

    const campoParam = String(req.query.campo || 'fecha_receta').toLowerCase();
    const campoFecha = (campoParam === 'created_at') ? 'created_at' : 'fecha_receta';
    const order = String(req.query.order || 'desc').toLowerCase() === 'asc' ? 'ASC' : 'DESC';

    const r = await p.request()
      .input('estado', estado)
      .input('paciente', paciente)
      .input('doctor', doctor)
      .input('desde', desde)
      .input('hasta', hasta)
      .input('offset', offset)
      .input('limit', limit)
      .query(`
        SELECT *
        FROM ${REC_TABLE}
        WHERE 1=1
          AND (@estado  IS NULL OR estado_receta  = @estado)
          AND (@paciente IS NULL OR id_paciente   = @paciente)
          AND (@doctor  IS NULL OR codigo_doctor  = @doctor)
          AND (@desde   IS NULL OR ${campoFecha} >= @desde)
          AND (@hasta   IS NULL OR ${campoFecha} < DATEADD(day, 1, @hasta))
        ORDER BY ${campoFecha} ${order}, codigo_receta DESC
        OFFSET @offset ROWS FETCH NEXT @limit ROWS ONLY;
      `);

    res.json(r.recordset);
  } catch (err) {
    console.error('GET /api/recetas', err);
    res.status(500).json({ error: 'DB_ERROR', message: err.message });
  }
});

// GET /api/recetas/estado/:estado
// Lista por estado con paginado y ventana de fechas opcional (?desde&hasta&limit&offset)
app.get('/api/recetas/estado/:estado', async (req, res) => {
  try {
    const p = await getPool();
    const estado = normalizeEstado(req.params.estado);
    if (!ESTADOS_DB.has(estado)) {
      return res.status(400).json({ error: 'INVALID_ESTADO', allowed: [...ESTADOS_DB] });
    }
    const limit  = Math.min(parseInt(req.query.limit  || '50', 10), 200);
    const offset = parseInt(req.query.offset || '0', 10);
    const desde  = (req.query.desde || '').trim() || null;
    const hasta  = (req.query.hasta || '').trim() || null;

    const r = await p.request()
      .input('estado', estado)
      .input('desde', desde)
      .input('hasta', hasta)
      .input('offset', offset)
      .input('limit', limit)
      .query(`
        SELECT *
        FROM ${REC_TABLE}
        WHERE estado_receta = @estado
          AND (@desde IS NULL OR fecha_receta >= @desde)
          AND (@hasta IS NULL OR fecha_receta < DATEADD(day, 1, @hasta))
        ORDER BY fecha_receta DESC, codigo_receta DESC
        OFFSET @offset ROWS FETCH NEXT @limit ROWS ONLY;
      `);

    res.json(r.recordset);
  } catch (err) {
    console.error('GET /api/recetas/estado/:estado', err);
    res.status(500).json({ error: 'DB_ERROR', message: err.message });
  }
});

// GET /api/recetas/fecha
// Filtro por ventana de tiempo (obligatorio al menos uno de: desde|hasta)
// ?desde=YYYY-MM-DD&hasta=YYYY-MM-DD&campo=fecha_receta|created_at&limit&offset
app.get('/api/recetas/fecha', async (req, res) => {
  try {
    const desde = (req.query.desde || '').trim() || null;
    const hasta = (req.query.hasta || '').trim() || null;
    if (!desde && !hasta) {
      return res.status(400).json({ error: 'MISSING_RANGE', message: 'Envia desde y/o hasta.' });
    }
    const p = await getPool();
    const limit  = Math.min(parseInt(req.query.limit  || '50', 10), 200);
    const offset = parseInt(req.query.offset || '0', 10);

    const campoParam = String(req.query.campo || 'fecha_receta').toLowerCase();
    const campoFecha = (campoParam === 'created_at') ? 'created_at' : 'fecha_receta';

    const r = await p.request()
      .input('desde', desde)
      .input('hasta', hasta)
      .input('offset', offset)
      .input('limit', limit)
      .query(`
        SELECT *
        FROM ${REC_TABLE}
        WHERE (@desde IS NULL OR ${campoFecha} >= @desde)
          AND (@hasta IS NULL OR ${campoFecha} < DATEADD(day, 1, @hasta))
        ORDER BY ${campoFecha} DESC, codigo_receta DESC
        OFFSET @offset ROWS FETCH NEXT @limit ROWS ONLY;
      `);

    res.json(r.recordset);
  } catch (err) {
    console.error('GET /api/recetas/fecha', err);
    res.status(500).json({ error: 'DB_ERROR', message: err.message });
  }
});

// GET /api/recetas/vencen
// Próximas a expirar por qr_exp (por defecto 7 días). Solo recetas ISSUED y QR no usado.
// ?dias=7&limit&offset
app.get('/api/recetas/vencen', async (req, res) => {
  try {
    const p = await getPool();
    const dias   = Math.max(1, Math.min(parseInt(req.query.dias || '7', 10), 90));
    const limit  = Math.min(parseInt(req.query.limit  || '50', 10), 200);
    const offset = parseInt(req.query.offset || '0', 10);

    const r = await p.request()
      .input('dias', dias)
      .input('offset', offset)
      .input('limit', limit)
      .query(`
        SELECT *
        FROM ${REC_TABLE}
        WHERE qr_exp IS NOT NULL
          AND qr_exp >  SYSUTCDATETIME()
          AND qr_exp <= DATEADD(day, @dias, SYSUTCDATETIME())
          AND UPPER(estado_receta) = 'ISSUED'
          AND (qr_used = 0 OR qr_used IS NULL)
        ORDER BY qr_exp ASC, codigo_receta DESC
        OFFSET @offset ROWS FETCH NEXT @limit ROWS ONLY;
      `);

    res.json(r.recordset);
  } catch (err) {
    console.error('GET /api/recetas/vencen', err);
    res.status(500).json({ error: 'DB_ERROR', message: err.message });
  }
});

// GET /api/recetas/exists
// Chequeo rápido: ?codigo_receta=... | ?rx_hash=...  (al menos uno)
// Respuesta: { exists: true/false }
app.get('/api/recetas/exists', async (req, res) => {
  try {
    const codigo = (req.query.codigo_receta || '').trim() || null;
    const hash   = (req.query.rx_hash       || '').trim() || null;
    if (!codigo && !hash) {
      return res.status(400).json({ error: 'MISSING_QUERY', message: 'Envia codigo_receta o rx_hash.' });
    }
    const p = await getPool();
    const r = await p.request()
      .input('codigo', codigo)
      .input('hash', hash)
      .query(`
        SELECT TOP 1 1 AS ok
        FROM ${REC_TABLE}
        WHERE (@codigo IS NOT NULL AND codigo_receta = @codigo)
           OR (@hash   IS NOT NULL AND rx_hash        = @hash);
      `);
    res.json({ exists: r.recordset.length > 0 });
  } catch (err) {
    console.error('GET /api/recetas/exists', err);
    res.status(500).json({ error: 'DB_ERROR', message: err.message });
  }
});



// GET /api/recetas/:id   (detalle por codigo_receta)
// ⚠️ Pon esta ruta **AL FINAL** del bloque de recetas, después de /estado, /fecha, /vencen, etc.
app.get('/api/recetas/:id', async (req, res) => {
  try {
    const p = await getPool();
    const r = await p.request().input('id', req.params.id).query(`
      SELECT r.*, p.nombre_pac, p.apellido_pac, d.nombre_doc, d.apellido_doc
      FROM ${REC_TABLE} r
      LEFT JOIN [MedPresc].dbo.[Paciente] p ON p.id_paciente = r.id_paciente
      LEFT JOIN [MedPresc].dbo.[Doctor]   d ON d.codigo_doctor = r.codigo_doctor
      WHERE r.codigo_receta = @id;
    `);
    if (!r.recordset.length) return res.status(404).json({ error: 'NOT_FOUND' });

    const x = r.recordset[0];
    const items = await fetchRecetaItems(p, x.codigo_receta);
    res.json({ ...x, items });
  } catch (err) {
    console.error('GET /api/recetas/:id', err);
    res.status(500).json({ error: 'DB_ERROR', message: err.message });
  }
});



//PROBAR/////////////////////////////////////////////////////////////////////////

// =========================
// Recetas - TX On-chain / Auditoría / Items / Reportes
// =========================
//const REC_TABLE      = '[MedPresc].dbo.[Receta]';
const ITEM_TABLE     = '[MedPresc].dbo.[medicamento_por_receta]';
const DISP_TABLE     = '[MedPresc].dbo.[medicamento_dispensado]'; // tu tabla de la captura

// ---- Helpers ----
function txCol(tipo) {
  const t = String(tipo || '').toLowerCase();
  if (t === 'create')   return 'onchain_tx_create';
  if (t === 'dispense') return 'onchain_tx_dispense';
  if (t === 'revoke')   return 'onchain_tx_revoke';
  return null;
}
function toBool(v){ return v === true || v === 1 || v === '1' || String(v).toLowerCase() === 'true'; }

// ========================================
// TX ON-CHAIN
// ========================================



// GET /api/recetas/:id/tx
app.get('/api/recetas/:id/tx', async (req,res)=>{
  try{
    const p = await getPool();
    const r = await p.request().input('id', req.params.id).query(`
      SELECT codigo_receta, estado_receta,
             onchain_tx_create, onchain_tx_dispense, onchain_tx_revoke,
             chain_id, channel_name, contract_name
      FROM ${REC_TABLE}
      WHERE codigo_receta = @id;
    `);
    if (!r.recordset.length) return res.status(404).json({error:'NOT_FOUND'});
    res.json(r.recordset[0]);
  }catch(err){
    console.error('GET /api/recetas/:id/tx', err);
    res.status(500).json({error:'DB_ERROR', message: err.message});
  }
});

// POST /api/onchain/callback
// Body sugerido:
// {
//   "codigo_receta": "RX-...",
//   "tipo": "create|dispense|revoke",
//   "tx_hash": "0x...",
//   "status": "confirmed|failed",
//   "chain_id": "xxx", "channel_name": "xxx", "contract_name": "xxx",
//   "qr_scanned_by_pharmacy_id": 101,   // opcional (dispense)
//   "items": [                          // opcional: para registrar en medicamento_dispensado
//     { "id_medicamento": 101, "nombre_medicamento": "Amoxicilina", "codigo_regulador": 1, "codigo_farmacia": 101, "actividad_sospechosa": false, "comentario": "OK" }
//   ],
//   "fecha_escaneado": "2025-09-01T12:00:00Z" // opcional
// }
app.post('/api/onchain/callback', async (req,res)=>{
  const b = req.body || {};
  try{
    const tipo = txCol(b.tipo);
    if (!b.codigo_receta || !tipo || !b.tx_hash){
      return res.status(400).json({error:'MISSING_FIELDS', message:'codigo_receta, tipo(create|dispense|revoke) y tx_hash son requeridos.'});
    }
    const status = String(b.status||'').toLowerCase();

    const p = await getPool();
    const tx = new sql.Transaction(p);
    await tx.begin();

    try{
      // actualizar columnas on-chain + chain metadata
      const reqU = new sql.Request(tx);
      reqU.input('id', b.codigo_receta)
          .input('hash', b.tx_hash)
          .input('chain', b.chain_id || null)
          .input('chan',  b.channel_name || null)
          .input('cn',    b.contract_name || null);

      // base query y, si es confirmed, aplicar efectos
      let q = `
        UPDATE ${REC_TABLE}
        SET ${tipo} = @hash,
            chain_id = COALESCE(@chain, chain_id),
            channel_name = COALESCE(@chan, channel_name),
            contract_name = COALESCE(@cn, contract_name)
      `;

      if (status === 'confirmed') {
        if (tipo === 'onchain_tx_dispense') {
          q += `,
            estado_receta = 'DISPENSED',
            qr_used = 1,
            qr_used_ts = COALESCE(qr_used_ts, SYSUTCDATETIME()),
            qr_scanned_by_pharmacy_id = COALESCE(@ph, qr_scanned_by_pharmacy_id)
          `;
          reqU.input('ph', b.qr_scanned_by_pharmacy_id || null);
        } else if (tipo === 'onchain_tx_revoke') {
          q += `,
            estado_receta = 'REVOKED'
          `;
        } // create: no cambia estado (permanece ISSUED)
      }
      q += ` OUTPUT INSERTED.* WHERE codigo_receta = @id;`;

      const upd = await reqU.query(q);
      if (!upd.recordset.length) throw new Error('NOT_FOUND');

      // si vino listado de items para registrar en medicamento_dispensado
      if (Array.isArray(b.items) && b.items.length){
        const when = b.fecha_escaneado ? new Date(b.fecha_escaneado) : new Date();

        for (const it of b.items){
          const rq = new sql.Request(tx);
          rq.input('idrec', sql.VarChar(40), b.codigo_receta)
            .input('idmed', sql.Int, it.id_medicamento)
            .input('nom',   sql.NVarChar(200), it.nombre_medicamento || null)
            .input('reg',   sql.Int, it.codigo_regulador || null)
            .input('far',   sql.Int, it.codigo_farmacia || null)
            .input('fesc',  sql.DateTime2, when)
            .input('susp',  sql.Bit, toBool(it.actividad_sospechosa) ? 1 : 0)
            .input('comm',  sql.NVarChar(500), it.comentario || null);
            await rq.query(`
            IF NOT EXISTS (
            SELECT 1
            FROM ${DISP_TABLE}
            WHERE id_receta = @idrec AND id_medicamento = @idmed
            )
            BEGIN
            INSERT INTO ${DISP_TABLE}
            (id_receta, id_medicamento, nombre_medicamento, codigo_regulador, codigo_farmacia,
            fecha_escaneado, actividad_sospechosa, comentario)
            VALUES (@idrec, @idmed, @nom, @reg, @far, @fesc, @susp, @comm);
            END
            `);
        }
      }

      await tx.commit();
      res.status(200).json({ ok:true });
    }catch(e){
      await tx.rollback();
      if (e.message === 'NOT_FOUND') return res.status(404).json({error:'NOT_FOUND'});
      throw e;
    }
  }catch(err){
    console.error('POST /api/onchain/callback', err);
    res.status(500).json({error:'DB_ERROR', message: err.message});
  }
});

// POST /api/recetas/:id/tx/retry?tipo=create|dispense|revoke
// Marca el tx para reintento: limpia el hash correspondiente. No cambia el estado.
app.post('/api/recetas/:id/tx/retry', async (req,res)=>{
  try{
    const col = txCol(req.query.tipo);
    if (!col) return res.status(400).json({error:'INVALID_TIPO', allowed:['create','dispense','revoke']});

    const p = await getPool();
    const r = await p.request()
      .input('id', req.params.id)
      .query(`
        UPDATE ${REC_TABLE}
        SET ${col} = NULL
        OUTPUT INSERTED.*
        WHERE codigo_receta = @id;
      `);
    if (!r.recordset.length) return res.status(404).json({error:'NOT_FOUND'});
    res.status(202).json({ ok:true, receta:r.recordset[0] });
  }catch(err){
    console.error('POST /api/recetas/:id/tx/retry', err);
    res.status(500).json({error:'DB_ERROR', message: err.message});
  }
});

// ========================================
// AUDITORÍA (derivada) / MÉTRICAS
// ========================================

// GET /api/recetas/:id/auditoria
// Construye un historial a partir de columnas de Receta + registros en medicamento_dispensado
app.get('/api/recetas/:id/auditoria', async (req,res)=>{
  try{
    const p = await getPool();
    const rec = await p.request().input('id', req.params.id).query(`
      SELECT *
      FROM ${REC_TABLE}
      WHERE codigo_receta = @id;
    `);
    if (!rec.recordset.length) return res.status(404).json({error:'NOT_FOUND'});
    const r = rec.recordset[0];

    const disp = await p.request().input('id', req.params.id).query(`
      SELECT *
      FROM ${DISP_TABLE}
      WHERE id_receta = @id
      ORDER BY fecha_escaneado ASC, id_medicamento ASC;
    `);

    const events = [];
    if (r.created_at) events.push({ type:'CREATED', ts:r.created_at, by:r.created_by ?? null });
    if (r.qr_jti)     events.push({ type:'QR_EMITIDO', ts:r.qr_exp, jti:r.qr_jti, exp:r.qr_exp });
    if (r.onchain_tx_create)   events.push({ type:'ONCHAIN_CREATE',   ts:r.created_at || null, tx:r.onchain_tx_create });
    if (r.qr_used_ts || r.qr_used) events.push({ type:'QR_USADO', ts:r.qr_used_ts || null, by_pharmacy:r.qr_scanned_by_pharmacy_id || null });
    if (r.onchain_tx_dispense) events.push({ type:'ONCHAIN_DISPENSE', ts:r.qr_used_ts || null, tx:r.onchain_tx_dispense });
    if (r.onchain_tx_revoke)   events.push({ type:'ONCHAIN_REVOKE',   ts:null, tx:r.onchain_tx_revoke });
    events.push({ type:'ESTADO_ACTUAL', ts:new Date(), estado:r.estado_receta });

    for (const d of disp.recordset){
      events.push({
        type:'DISP_ITEM',
        ts:d.fecha_escaneado,
        item:{
          id_medicamento:d.id_medicamento,
          nombre_medicamento:d.nombre_medicamento,
          codigo_farmacia:d.codigo_farmacia,
          codigo_regulador:d.codigo_regulador,
          actividad_sospechosa:d.actividad_sospechosa,
          comentario:d.comentario
        }
      });
    }

    res.json({ codigo_receta:r.codigo_receta, events });
  }catch(err){
    console.error('GET /api/recetas/:id/auditoria', err);
    res.status(500).json({error:'DB_ERROR', message: err.message});
  }
});


// GET /api/reportes/recetas/resumen?desde&hasta&doctor&farmacia
app.get('/api/reportes/recetas/resumen', async (req,res)=>{
  try{
    const p = await getPool();
    const doctor   = req.query.doctor   ? parseInt(req.query.doctor,10)   : null;
    const farmacia = req.query.farmacia ? parseInt(req.query.farmacia,10) : null;
    const desde    = (req.query.desde || '').trim() || null;
    const hasta    = (req.query.hasta || '').trim() || null;

    // filtros base + filtro por farmacia via EXISTS en medicamento_dispensado
    const existsFarm = farmacia ? `
      AND EXISTS (
        SELECT 1 FROM ${DISP_TABLE} d
        WHERE d.id_receta = r.codigo_receta AND d.codigo_farmacia = @farmacia
      )
    ` : '';

    // emitidas
    const qEmit = await p.request()
      .input('doctor', doctor).input('farmacia', farmacia)
      .input('desde', desde).input('hasta', hasta)
      .query(`
        SELECT COUNT(*) AS c
        FROM ${REC_TABLE} r
        WHERE UPPER(r.estado_receta)='ISSUED'
          AND (@doctor IS NULL OR r.codigo_doctor = @doctor)
          AND (@desde  IS NULL OR r.fecha_receta >= @desde)
          AND (@hasta  IS NULL OR r.fecha_receta < DATEADD(day,1,@hasta))
          ${existsFarm};
      `);

    // dispensadas
    const qDisp = await p.request()
      .input('doctor', doctor).input('farmacia', farmacia)
      .input('desde', desde).input('hasta', hasta)
      .query(`
        SELECT COUNT(*) AS c
        FROM ${REC_TABLE} r
        WHERE UPPER(r.estado_receta)='DISPENSED'
          AND (@doctor IS NULL OR r.codigo_doctor = @doctor)
          AND (@desde  IS NULL OR r.fecha_receta >= @desde)
          AND (@hasta  IS NULL OR r.fecha_receta < DATEADD(day,1,@hasta))
          ${existsFarm};
      `);

    // revocadas
    const qRev = await p.request()
      .input('doctor', doctor).input('farmacia', farmacia)
      .input('desde', desde).input('hasta', hasta)
      .query(`
        SELECT COUNT(*) AS c
        FROM ${REC_TABLE} r
        WHERE UPPER(r.estado_receta)='REVOKED'
          AND (@doctor IS NULL OR r.codigo_doctor = @doctor)
          AND (@desde  IS NULL OR r.fecha_receta >= @desde)
          AND (@hasta  IS NULL OR r.fecha_receta < DATEADD(day,1,@hasta))
          ${existsFarm};
      `);

    // tiempo medio a dispensar (horas)
    const qAvg = await p.request()
      .input('doctor', doctor).input('farmacia', farmacia)
      .input('desde', desde).input('hasta', hasta)
      .query(`
        SELECT AVG(CAST(DATEDIFF(second, r.fecha_receta, r.qr_used_ts) AS float)) AS avg_sec
        FROM ${REC_TABLE} r
        WHERE UPPER(r.estado_receta)='DISPENSED'
          AND r.qr_used_ts IS NOT NULL
          AND (@doctor IS NULL OR r.codigo_doctor = @doctor)
          AND (@desde  IS NULL OR r.fecha_receta >= @desde)
          AND (@hasta  IS NULL OR r.fecha_receta < DATEADD(day,1,@hasta))
          ${existsFarm};
      `);

    const avgSec = qAvg.recordset[0].avg_sec;
    res.json({
      filtros: { desde, hasta, doctor, farmacia },
      emitidas:   qEmit.recordset[0].c,
      dispensadas:qDisp.recordset[0].c,
      revocadas:  qRev.recordset[0].c,
      tiempo_medio_dispensar_horas: avgSec != null ? (avgSec/3600) : null
    });
  }catch(err){
    console.error('GET /api/reportes/recetas/resumen', err);
    res.status(500).json({error:'DB_ERROR', message: err.message});
  }
});

// ========================================
// ITEMS (medicamento_por_receta) SUBRECURSO
// ========================================

// POST /api/recetas/:id/items
// Body: { codigo_medicamento, nombre_medicamento?, dosis_medicamento? }
app.post('/api/recetas/:id/items', async (req, res) => {
  try {
    const b = req.body || {};
    const cmInt = parseInt(b.codigo_medicamento, 10);
    if (isNaN(cmInt)) {
      return res.status(400).json({
        error: 'MISSING_FIELDS',
        message: 'codigo_medicamento es requerido y debe ser numérico.'
      });
    }

    const p = await getPool();
    const r = await p.request()
      .input('id',  req.params.id)                    // codigo_receta
      .input('cm',  cmInt)                            // codigo_medicamento
      .input('nom', b.nombre_medicamento || null)
      .input('dos', b.dosis_medicamento  || null)
      .query(`
        MERGE ${ITEM_TABLE} AS T
        USING (
          SELECT @id AS codigo_receta, @cm AS codigo_medicamento
        ) AS S
        ON (T.codigo_receta = S.codigo_receta AND T.codigo_medicamento = S.codigo_medicamento)
        WHEN MATCHED THEN
          UPDATE SET
            nombre_medicamento = COALESCE(@nom, T.nombre_medicamento),
            dosis_medicamento  = COALESCE(@dos, T.dosis_medicamento)
        WHEN NOT MATCHED THEN
          INSERT (codigo_medicamento, codigo_receta, nombre_medicamento, dosis_medicamento)
          VALUES (@cm, @id, @nom, @dos)
        OUTPUT $action AS accion, inserted.*;
      `);

    const row = r.recordset[0];
    const accion = (row?.accion || '').toUpperCase(); // 'INSERT' | 'UPDATE'
    const status = accion === 'INSERT' ? 201 : 200;
    // quitamos 'accion' del payload de respuesta
    if (row && 'accion' in row) delete row.accion;
    return res.status(status).json(row || { ok: true });
  } catch (err) {
    console.error('POST /api/recetas/:id/items', err);
    return res.status(500).json({ error: 'DB_ERROR', message: err.message });
  }
});

// POST /api/recetas/:id/items
// Body: { codigo_medicamento, nombre_medicamento?, dosis_medicamento? }
app.post('/api/recetas/:id/items', async (req,res)=>{
  try{
    const b = req.body || {};
    if (b.codigo_medicamento == null)
      return res.status(400).json({error:'MISSING_FIELDS', message:'codigo_medicamento es requerido.'});

    const p = await getPool();
    const r = await p.request()
      .input('id',   req.params.id)
      .input('cm',   b.codigo_medicamento)
      .input('nom',  b.nombre_medicamento || null)
      .input('dos',  b.dosis_medicamento  || null)
      .query(`
        INSERT INTO ${ITEM_TABLE}
          (codigo_medicamento, codigo_receta, nombre_medicamento, dosis_medicamento)
        OUTPUT INSERTED.*
        VALUES (@cm, @id, @nom, @dos);
      `);
    res.status(201).json(r.recordset[0]);
  }catch(err){
    console.error('POST /api/recetas/:id/items', err);
    res.status(500).json({error:'DB_ERROR', message: err.message});
  }
});

// PATCH /api/recetas/:id/items/:codigo_medicamento
// Body: { nombre_medicamento?, dosis_medicamento? }
app.patch('/api/recetas/:id/items/:codigo_medicamento', async (req,res)=>{
  try{
    const b = req.body || {};
    const sets = [];
    if (b.nombre_medicamento !== undefined) sets.push('nombre_medicamento = @nom');
    if (b.dosis_medicamento  !== undefined) sets.push('dosis_medicamento  = @dos');
    if (!sets.length) return res.status(400).json({error:'EMPTY_BODY', message:'Nada que actualizar.'});

    const p = await getPool();
    const r = await p.request()
      .input('id',  req.params.id)
      .input('cm',  parseInt(req.params.codigo_medicamento,10))
      .input('nom', b.nombre_medicamento ?? null)
      .input('dos', b.dosis_medicamento  ?? null)
      .query(`
        UPDATE ${ITEM_TABLE}
        SET ${sets.join(', ')}
        OUTPUT INSERTED.*
        WHERE codigo_receta = @id AND codigo_medicamento = @cm;
      `);
    if (!r.recordset.length) return res.status(404).json({error:'NOT_FOUND'});
    res.json(r.recordset[0]);
  }catch(err){
    console.error('PATCH /api/recetas/:id/items/:codigo_medicamento', err);
    res.status(500).json({error:'DB_ERROR', message: err.message});
  }
});

// DELETE /api/recetas/:id/items/:codigo_medicamento
app.delete('/api/recetas/:id/items/:codigo_medicamento', async (req,res)=>{
  try{
    const p = await getPool();
    const r = await p.request()
      .input('id', req.params.id)
      .input('cm', parseInt(req.params.codigo_medicamento,10))
      .query(`
        DELETE FROM ${ITEM_TABLE}
        OUTPUT DELETED.*
        WHERE codigo_receta = @id AND codigo_medicamento = @cm;
      `);
    if (!r.recordset.length) return res.status(404).json({error:'NOT_FOUND'});
    res.json({ ok:true, deleted:r.recordset[0] });
  }catch(err){
    console.error('DELETE /api/recetas/:id/items/:codigo_medicamento', err);
    res.status(500).json({error:'DB_ERROR', message: err.message});
  }
});

// ========================================
// Adjuntos / Firma (placeholders 501)
// ========================================

app.post('/api/recetas/:id/adjuntos', (_req,res)=>{
  res.status(501).json({ error:'NOT_IMPLEMENTED', message:'Integrar con Azure Blob + tabla receta_adjunto.' });
});
app.get('/api/recetas/:id/adjuntos', (_req,res)=>{
  res.status(501).json({ error:'NOT_IMPLEMENTED', message:'Integrar con Azure Blob + tabla receta_adjunto.' });
});
app.post('/api/recetas/:id/firma', (_req,res)=>{
  res.status(501).json({ error:'NOT_IMPLEMENTED', message:'Agregar almacenamiento de firma digital / hash de PDF.' });
});
app.get('/api/recetas/:id/firmada', (_req,res)=>{
  res.status(501).json({ error:'NOT_IMPLEMENTED', message:'Requiere tabla o columnas para estado de firma.' });
});



// =========================
// Dispensación - MVP + Opcionales
// =========================
const DISPENSA_TABLE = '[MedPresc].dbo.[Dispensacion]'; // cámbialo si tu tabla se llama distinto
const RECETA_TABLE  = '[MedPresc].dbo.[Receta]';

// Utiles
function toBool(v){ return v === true || v === 1 || v === '1' || String(v).toLowerCase() === 'true'; }

// ---------- MVP ----------

// POST /api/dispensacion
// Body: { codigo_receta, onchain_tx_dispense?, codigo_farmacia?, fecha_escaneado?, actividad_sospechosa?, comentario? }
app.post('/api/dispensacion', async (req, res) => {
  const b = req.body || {};
  try {
    if (!b.codigo_receta) {
      return res.status(400).json({ error: 'MISSING_FIELDS', message: 'codigo_receta es requerido.' });
    }
    const p = await getPool();
    const cur = await p.request().input('id', b.codigo_receta).query(`
      SELECT codigo_receta, estado_receta, qr_used
      FROM ${RECETA_TABLE}
      WHERE codigo_receta = @id;
    `);
    if (!cur.recordset.length) return res.status(404).json({ error: 'NOT_FOUND', message: 'Receta no existe.' });

    const R = cur.recordset[0];
    if (String(R.estado_receta).toUpperCase() !== 'ISSUED' || R.qr_used) {
      return res.status(409).json({ error: 'NO_APLICA', message: 'La receta no está ISSUED o ya fue usada.' });
    }

    const tx = new sql.Transaction(p);
    await tx.begin();
    try {
      const when = b.fecha_escaneado ? new Date(b.fecha_escaneado) : new Date();

      // 1) registrar dispensación (SOLO Dispensacion)
        const ins = await new sql.Request(tx)
        .input('cod',  sql.VarChar(40), b.codigo_receta)
        .input('fesc', sql.DateTime2, when)
        .input('susp', sql.Bit, toBool(b.actividad_sospechosa) ? 1 : 0)
        .input('comm', sql.NVarChar(500), b.comentario ?? null)
        .query(`
        INSERT INTO ${DISPENSA_TABLE}
        (codigo_receta, fecha_escaneado, actividad_sospechosa, comentario)
        OUTPUT INSERTED.*
       VALUES (@cod, @fesc, @susp, @comm);
      `);

      // 2) marcar receta DISPENSED + QR usado + farmacia opcional
      const up = await new sql.Request(tx)
        .input('id', sql.VarChar(40), b.codigo_receta)
        .input(' tx', sql.VarChar(128), b.onchain_tx_dispense || null)
        .input('ph', sql.Int, b.codigo_farmacia ?? null)
        .query(`
          UPDATE ${RECETA_TABLE}
          SET estado_receta = 'DISPENSED',
              onchain_tx_dispense = COALESCE(@tx, onchain_tx_dispense),
              qr_used = 1,
              qr_used_ts = SYSUTCDATETIME(),
              qr_scanned_by_pharmacy_id = COALESCE(@ph, qr_scanned_by_pharmacy_id)
          OUTPUT INSERTED.*
          WHERE codigo_receta = @id;
        `);

      await tx.commit();
      res.status(201).json({ dispensacion: ins.recordset[0], receta: up.recordset[0] });
    } catch (e) {
      await tx.rollback();
      throw e;
    }
  } catch (err) {
    console.error('POST /api/dispensacion', err);
    res.status(500).json({ error: 'DB_ERROR', message: err.message });
  }
});

// GET/api/dispensacion
// Filtros: ?receta=&farmacia=&sospechosa=&desde=YYYY-MM-DD&hasta=YYYY-MM-DD&limit=50&offset=0&order=desc
app.get('/api/dispensacion', async (req, res) => {
  try {
    const p = await getPool();
    const limit  = Math.min(parseInt(req.query.limit  || '50', 10), 200);
    const offset = parseInt(req.query.offset || '0', 10);
    const receta = (req.query.receta || '').trim() || null;
    const farm   = req.query.farmacia ? parseInt(req.query.farmacia, 10) : null; // via Receta.qr_scanned_by_pharmacy_id
    const sospe  = (req.query.sospechosa ?? '').toString().toLowerCase();
    const sospeFlag = (sospe === 'true' ? 1 : (sospe === 'false' ? 0 : null));
    const desde  = (req.query.desde || '').trim() || null;
    const hasta  = (req.query.hasta || '').trim() || null;
    const order  = String(req.query.order || 'desc').toLowerCase() === 'asc' ? 'ASC' : 'DESC';

    const r = await p.request()
      .input('rec', receta)
      .input('farm', farm)
      .input('susp', sospeFlag)
      .input('desde', desde)
      .input('hasta', hasta)
      .input('offset', offset)
      .input('limit', limit)
      .query(`
        SELECT d.*, r.qr_scanned_by_pharmacy_id AS codigo_farmacia
        FROM ${DISPENSA_TABLE} d
        LEFT JOIN ${RECETA_TABLE} r ON r.codigo_receta = d.codigo_receta
        WHERE 1=1
          AND (@rec  IS NULL OR d.codigo_receta = @rec)
          AND (@farm IS NULL OR r.qr_scanned_by_pharmacy_id = @farm)
          AND (@susp IS NULL OR d.actividad_sospechosa = @susp)
          AND (@desde IS NULL OR d.fecha_escaneado >= @desde)
          AND (@hasta IS NULL OR d.fecha_escaneado < DATEADD(day, 1, @hasta))
        ORDER BY d.fecha_escaneado ${order}, d.num_dispensacion DESC
        OFFSET @offset ROWS FETCH NEXT @limit ROWS ONLY;
      `);

    res.json(r.recordset);
  } catch (err) {
    console.error('GET /api/dispensacion', err);
    res.status(500).json({ error: 'DB_ERROR', message: err.message });
  }
});

// GET /api/dispensacion/count  (mismos filtros que listado)
app.get('/api/dispensacion/count', async (req, res) => {
  try {
    const p = await getPool();
    const receta = (req.query.receta || '').trim() || null;
    const farm   = req.query.farmacia ? parseInt(req.query.farmacia, 10) : null;
    const sospe  = (req.query.sospechosa ?? '').toString().toLowerCase();
    const sospeFlag = (sospe === 'true' ? 1 : (sospe === 'false' ? 0 : null));
    const desde  = (req.query.desde || '').trim() || null;
    const hasta  = (req.query.hasta || '').trim() || null;

    const r = await p.request()
      .input('rec', receta)
      .input('farm', farm)
      .input('susp', sospeFlag)
      .input('desde', desde)
      .input('hasta', hasta)
      .query(`
        SELECT COUNT(*) AS total
        FROM ${DISPENSA_TABLE} d
        LEFT JOIN ${RECETA_TABLE} r ON r.codigo_receta = d.codigo_receta
        WHERE 1=1
          AND (@rec  IS NULL OR d.codigo_receta = @rec)
          AND (@farm IS NULL OR r.qr_scanned_by_pharmacy_id = @farm)
          AND (@susp IS NULL OR d.actividad_sospechosa = @susp)
          AND (@desde IS NULL OR d.fecha_escaneado >= @desde)
          AND (@hasta IS NULL OR d.fecha_escaneado < DATEADD(day, 1, @hasta));
      `);

    res.json({ total: r.recordset[0].total });
  } catch (err) {
    console.error('GET /api/dispensacion/count', err);
    res.status(500).json({ error:'DB_ERROR', message: err.message });
  }
});

// GET /api/dispensacion/:num  (detalle por PK)
app.get('/api/dispensacion/:num', async (req, res) => {
  try {
    const p = await getPool();
    const n = parseInt(req.params.num, 10);

    const r = await p.request()
      .input('num', isNaN(n) ? null : n)
      .query(`
        SELECT d.*, r.qr_scanned_by_pharmacy_id AS codigo_farmacia
        FROM ${DISPENSA_TABLE} d           -- << aquí va Dispensacion
        LEFT JOIN ${RECETA_TABLE} r
          ON r.codigo_receta = d.codigo_receta
        WHERE d.num_dispensacion = @num;   -- PK correcto
      `);

    if (!r.recordset.length) return res.status(404).json({ error: 'NOT_FOUND' });
    res.json(r.recordset[0]);
  } catch (err) {
    console.error('GET /api/dispensacion/:num', err);
    res.status(500).json({ error: 'DB_ERROR', message: err.message });
  }
});



// GET /api/recetas/:id/dispensacion  (historial por receta)
app.get('/api/recetas/:id/dispensacion', async (req, res) => {
  try {
    const p = await getPool();
    const r = await p.request()
      .input('id', req.params.id)
      .query(`
        SELECT *
        FROM ${DISPENSA_TABLE}
        WHERE codigo_receta = @id
        ORDER BY fecha_escaneado DESC, num_dispensacion DESC;
      `);
    res.json(r.recordset);
  } catch (err) {
    console.error('GET /api/recetas/:id/dispensacion', err);
    res.status(500).json({ error: 'DB_ERROR', message: err.message });
  }
});

// ---------- Opcionales recomendados ----------

// POST /api/dispensacion/qr/:jti  (escaneo directo por QR)
app.post('/api/dispensacion/qr/:jti', async (req, res) => {
  try {
    const b = req.body || {};
    const p = await getPool();
    const cur = await p.request().input('jti', req.params.jti).query(`
      SELECT *
      FROM ${RECETA_TABLE}
      WHERE qr_jti = @jti;
    `);
    if (!cur.recordset.length) return res.status(404).json({ error: 'NOT_FOUND', message: 'QR no encontrado' });

    const x = cur.recordset[0];
    const now = new Date();
    const exp = x.qr_exp ? new Date(x.qr_exp) : null;
    const razones = [];
    if (!exp) razones.push('SIN_EXPIRACION');
    if (exp && exp <= now) razones.push('QR_VENCIDO');
    if (x.qr_used) razones.push('QR_USADO');
    if (String(x.estado_receta).toUpperCase() !== 'ISSUED') razones.push('NO_ISSUED');
    if (razones.length) return res.status(409).json({ error: 'QR_INVALIDO', razones });

    const tx = new sql.Transaction(p);
    await tx.begin();
    try {
      const when = b.fecha_escaneado ? new Date(b.fecha_escaneado) : new Date();
      const ins = await new sql.Request(tx)
        .input('cod',  sql.VarChar(40), x.codigo_receta)
        .input('fesc', sql.DateTime2, when)
        .input('susp', sql.Bit, toBool(b.actividad_sospechosa) ? 1 : 0)
        .input('comm', sql.NVarChar(500), b.comentario ?? null)
        .query(`
          INSERT INTO ${DISPENSA_TABLE}
            (codigo_receta, fecha_escaneado, actividad_sospechosa, comentario)
          OUTPUT INSERTED.*
          VALUES (@cod, @fesc, @susp, @comm);
        `);

      const up = await new sql.Request(tx)
        .input('id', sql.VarChar(40), x.codigo_receta)
        .input('tx', sql.VarChar(128), b.onchain_tx_dispense || null)
        .input('ph', sql.Int, b.codigo_farmacia ?? null)
        .query(`
          UPDATE ${RECETA_TABLE}
          SET estado_receta = 'DISPENSED',
              onchain_tx_dispense = COALESCE(@tx, onchain_tx_dispense),
              qr_used = 1,
              qr_used_ts = SYSUTCDATETIME(),
              qr_scanned_by_pharmacy_id = COALESCE(@ph, qr_scanned_by_pharmacy_id)
          OUTPUT INSERTED.*
          WHERE codigo_receta = @id;
        `);

      await tx.commit();
      res.status(201).json({ dispensacion: ins.recordset[0], receta: up.recordset[0] });
    } catch (e) {
      await tx.rollback();
      throw e;
    }
  } catch (err) {
    console.error('POST /api/dispensacion/qr/:jti', err);
    res.status(500).json({ error: 'DB_ERROR', message: err.message });
  }
});



// PATCH /api/dispensacion/:num  (actualiza sospecha/comentario)
app.patch('/api/dispensacion/:num', async (req, res) => {
  try {
    const b = req.body || {};
    const sets = [];
    if (b.actividad_sospechosa !== undefined) sets.push('actividad_sospechosa = @susp');
    if (b.comentario !== undefined)           sets.push('comentario = @comm');
    if (!sets.length) return res.status(400).json({ error:'EMPTY_BODY', message:'Nada que actualizar.' });

    const p = await getPool();
    const r = await p.request()
      .input('num', parseInt(req.params.num,10))
      .input('susp', b.actividad_sospechosa === undefined ? null : (toBool(b.actividad_sospechosa) ? 1 : 0))
      .input('comm', b.comentario ?? null)
      .query(`
        UPDATE ${DISPENSA_TABLE}
        SET ${sets.join(', ')}
        OUTPUT INSERTED.*
        WHERE num_dispensacion = @num;
      `);
    if (!r.recordset.length) return res.status(404).json({ error:'NOT_FOUND' });
    res.json(r.recordset[0]);
  } catch (err) {
    console.error('PATCH /api/dispensacion/:num', err);
    res.status(500).json({ error: 'DB_ERROR', message: err.message });
  }
});



// GET /api/reportes/dispensacion/resumen?desde&hasta&farmacia&doctor
app.get('/api/reportes/dispensacion/resumen', async (req, res) => {
  try {
    const p = await getPool();
    const desde    = (req.query.desde || '').trim() || null;
    const hasta    = (req.query.hasta || '').trim() || null;
    const farmacia = req.query.farmacia ? parseInt(req.query.farmacia,10) : null;
    const doctor   = req.query.doctor   ? parseInt(req.query.doctor,10)   : null;

    const baseFilter = `
      WHERE 1=1
        ${desde ? 'AND d.fecha_escaneado >= @desde' : ''}
        ${hasta ? 'AND d.fecha_escaneado < DATEADD(day,1,@hasta)' : ''}
        ${farmacia ? 'AND r.qr_scanned_by_pharmacy_id = @farmacia' : ''}
        ${doctor ? 'AND r.codigo_doctor = @doctor' : ''}
    `;

    // Totales
    const tot = await p.request()
      .input('desde', desde).input('hasta', hasta)
      .input('farmacia', farmacia).input('doctor', doctor)
      .query(`
        SELECT
          COUNT(*) AS total,
          SUM(CASE WHEN d.actividad_sospechosa=1 THEN 1 ELSE 0 END) AS sospechosas
        FROM ${DISPENSA_TABLE} d
        JOIN ${RECETA_TABLE} r ON r.codigo_receta = d.codigo_receta
        ${baseFilter};
      `);

    // Por farmacia
    const porFarm = await p.request()
      .input('desde', desde).input('hasta', hasta)
      .input('farmacia', farmacia).input('doctor', doctor)
      .query(`
        SELECT r.qr_scanned_by_pharmacy_id AS farmacia,
               COUNT(*) AS total,
               SUM(CASE WHEN d.actividad_sospechosa=1 THEN 1 ELSE 0 END) AS sospechosas
        FROM ${DISPENSA_TABLE} d
        JOIN ${RECETA_TABLE} r ON r.codigo_receta = d.codigo_receta
        ${baseFilter}
        GROUP BY r.qr_scanned_by_pharmacy_id
        ORDER BY total DESC;
      `);

    // Por doctor
    const porDoc = await p.request()
      .input('desde', desde).input('hasta', hasta)
      .input('farmacia', farmacia).input('doctor', doctor)
      .query(`
        SELECT r.codigo_doctor AS doctor,
               COUNT(*) AS total,
               SUM(CASE WHEN d.actividad_sospechosa=1 THEN 1 ELSE 0 END) AS sospechosas
        FROM ${DISPENSA_TABLE} d
        JOIN ${RECETA_TABLE} r ON r.codigo_receta = d.codigo_receta
        ${baseFilter}
        GROUP BY r.codigo_doctor
        ORDER BY total DESC;
      `);

    res.json({
      filtros: { desde, hasta, farmacia, doctor },
      total: tot.recordset[0]?.total ?? 0,
      sospechosas: tot.recordset[0]?.sospechosas ?? 0,
      por_farmacia: porFarm.recordset,
      por_doctor: porDoc.recordset
    });
  } catch (err) {
    console.error('GET /api/reportes/dispensacion/resumen', err);
    res.status(500).json({ error:'DB_ERROR', message: err.message });
  }
});

// DELETE /api/dispensacion/:num  (opcional, sin revert por defecto)
app.delete('/api/dispensacion/:num', async (req, res) => {
  try {
    const p = await getPool();
    const r = await p.request()
      .input('num', parseInt(req.params.num,10))
      .query(`
        DELETE FROM ${DISPENSA_TABLE}
        OUTPUT DELETED.*
        WHERE num_dispensacion = @num;
      `);
    if (!r.recordset.length) return res.status(404).json({ error:'NOT_FOUND' });
    res.json({ ok:true, deleted: r.recordset[0] });
  } catch (err) {
    console.error('DELETE /api/dispensacion/:num', err);
    res.status(500).json({ error:'DB_ERROR', message: err.message });
  }
});

const port = process.env.PORT || 3000;
app.listen(port, () => console.log(`Listening on port ${port}...`));