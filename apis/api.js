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

// ===================== CONFIGURACIÓN DIRECTA =====================
const ACCESS_TOKEN_SECRET    = "tu_super_secreto_access";   // string secreto
const REFRESH_TOKEN_SECRET   = "tu_super_secreto_refresh";  // string secreto
const MFA_CHALLENGE_SECRET   = "tu_super_secreto_mfa";      // string secreto
const ACCESS_TOKEN_TTL       = "15m";                       // string válido para JWT
const REFRESH_TOKEN_TTL_DAYS = 15;                          // número de días
const ADMIN_REGISTER_SECRET  = "superclave-pararegistrar";  // clave admin
// =================================================================


// ==== AUTH & MFA (agregar debajo de tus rutas, antes del app.listen) ====
// ==== SHIMS sin dependencias externas (usa solo Node crypto) ====
//const crypto = require('crypto');

// ---------- Utils comunes ----------
function parseDurationToSeconds(s) {
  if (typeof s === 'number') return s;
  const m = String(s).match(/^(\d+)\s*([smhd])?$/i);
  if (!m) return 0;
  const n = parseInt(m[1], 10);
  const unit = (m[2] || 's').toLowerCase();
  const mult = unit === 's' ? 1 : unit === 'm' ? 60 : unit === 'h' ? 3600 : 86400;
  return n * mult;
}
function b64urlEncode(buf) {
  return Buffer.from(buf).toString('base64').replace(/=/g,'').replace(/\+/g,'-').replace(/\//g,'_');
}
function b64urlDecode(s) {
  s = s.replace(/-/g, '+').replace(/_/g, '/');
  const pad = 4 - (s.length % 4 || 4);
  return Buffer.from(s + '='.repeat(pad), 'base64');
}

// ---------- JWT (HS256) ----------
const jwt = {
  sign(payload, secret, opts = {}) {
    const header = { alg: 'HS256', typ: 'JWT' };
    const iat = Math.floor(Date.now() / 1000);
    const exp = opts.expiresIn ? iat + parseDurationToSeconds(opts.expiresIn) : undefined;
    const body = { ...payload, iat, ...(exp ? { exp } : {}) };
    const encHeader = b64urlEncode(JSON.stringify(header));
    const encPayload = b64urlEncode(JSON.stringify(body));
    const data = `${encHeader}.${encPayload}`;
    const sig = crypto.createHmac('sha256', String(secret)).update(data).digest('base64')
      .replace(/=/g,'').replace(/\+/g,'-').replace(/\//g,'_');
    return `${data}.${sig}`;
  },
  verify(token, secret) {
    const [h, p, s] = String(token).split('.');
    if (!h || !p || !s) throw new Error('JWT_MALFORMED');
    const data = `${h}.${p}`;
    const sig = crypto.createHmac('sha256', String(secret)).update(data).digest('base64')
      .replace(/=/g,'').replace(/\+/g,'-').replace(/\//g,'_');
    // timing-safe compare
    const ok = crypto.timingSafeEqual(Buffer.from(s), Buffer.from(sig));
    if (!ok) throw new Error('JWT_BADSIG');
    const payload = JSON.parse(b64urlDecode(p).toString('utf8'));
    if (payload.exp && Math.floor(Date.now()/1000) >= payload.exp) throw new Error('JWT_EXPIRED');
    return payload;
  },
  decode(token) {
    const parts = String(token).split('.');
    if (parts.length < 2) return null;
    try { return JSON.parse(b64urlDecode(parts[1]).toString('utf8')); } catch { return null; }
  }
};

// ---------- Password hashing (scrypt) ----------
// ---------- Password hashing (scrypt) — PARCHE SEGURO DE MEMORIA ----------
const SCRYPT_N  = 1 << 14; // 16384 (≈16 MB con r=8)
const SCRYPT_R  = 8;
const SCRYPT_P  = 1;
function scryptOptsFor(N = SCRYPT_N) {
  return { N, r: SCRYPT_R, p: SCRYPT_P, maxmem: 128 * N * SCRYPT_R + 1024 * 1024 };
}

const bcrypt = {
  async hash(password, _cost = 12) {
    // Usamos parámetros conservadores para no pasar el límite de memoria
    const salt = crypto.randomBytes(16);
    const opts = scryptOptsFor(SCRYPT_N);
    const dk = await new Promise((res, rej) =>
      crypto.scrypt(String(password), salt, 32, opts, (e, k) => e ? rej(e) : res(k))
    );
    // Formato: scrypt$N$saltHex$hashHex
    return `scrypt$${SCRYPT_N}$${salt.toString('hex')}$${Buffer.from(dk).toString('hex')}`;
  },

  async compare(password, stored) {
    try {
      const [algo, Nstr, saltHex, hashHex] = String(stored).split('$');
      if (algo !== 'scrypt') return false;
      const N = parseInt(Nstr, 10);
      const salt = Buffer.from(saltHex, 'hex');
      const opts = scryptOptsFor(N); // usa el N almacenado
      const dk = await new Promise((res, rej) =>
        crypto.scrypt(String(password), salt, 32, opts, (e, k) => e ? rej(e) : res(k))
      );
      return crypto.timingSafeEqual(dk, Buffer.from(hashHex, 'hex'));
    } catch {
      return false;
    }
  }
};


// ---------- TOTP (RFC 6238) ----------
function base32Encode(buf) {
  const alphabet = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ234567';
  let bits = 0, value = 0, out = '';
  for (const byte of buf) {
    value = (value << 8) | byte; bits += 8;
    while (bits >= 5) { out += alphabet[(value >>> (bits - 5)) & 31]; bits -= 5; }
  }
  if (bits > 0) out += alphabet[(value << (5 - bits)) & 31];
  return out;
}
function base32Decode(str) {
  const alphabet = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ234567';
  const map = Object.fromEntries(alphabet.split('').map((c,i)=>[c,i]));
  let bits = 0, value = 0; const out = [];
  str = String(str).replace(/=+$/,'').toUpperCase();
  for (const ch of str) {
    if (map[ch] === undefined) continue;
    value = (value << 5) | map[ch]; bits += 5;
    if (bits >= 8) { out.push((value >>> (bits - 8)) & 255); bits -= 8; }
  }
  return Buffer.from(out);
}
function hotp(secretBuf, counter, digits = 6) {
  const c = Buffer.alloc(8);
  for (let i = 0; i < 8; i++) c[7 - i] = counter & 0xff, counter >>= 8;
  const h = crypto.createHmac('sha1', secretBuf).update(c).digest();
  const offset = h[19] & 0xf;
  const code = ((h[offset] & 0x7f) << 24) | ((h[offset+1] & 0xff) << 16) |
               ((h[offset+2] & 0xff) << 8) | (h[offset+3] & 0xff);
  const mod = 10 ** digits;
  return String(code % mod).padStart(digits, '0');
}
const speakeasy = {
  generateSecret({ length = 20, name = 'App' } = {}) {
    const buf = crypto.randomBytes(length);
    const base32 = base32Encode(buf);
    const otpauth_url = `otpauth://totp/${encodeURIComponent(name)}?secret=${base32}&issuer=${encodeURIComponent('MedPresc')}`;
    return { base32, otpauth_url };
  },
  totp: {
    verify({ secret, encoding = 'base32', token, window = 1, step = 30 }) {
      const secretBuf = encoding === 'base32' ? base32Decode(secret) : Buffer.from(secret, encoding);
      const now = Math.floor(Date.now() / 1000);
      const counter = Math.floor(now / step);
      token = String(token).padStart(6, '0');
      for (let w = -window; w <= window; w++) {
        if (hotp(secretBuf, counter + w) === token) return true;
      }
      return false;
    }
  }
};


// Tablas auxiliares para refresh y reset (se crean si no existen)
async function ensureAuthTables() {
  const p = await getPool();
  await p.request().query(`
IF OBJECT_ID('dbo.RefreshToken','U') IS NULL
BEGIN
  CREATE TABLE dbo.RefreshToken(
    id INT IDENTITY(1,1) PRIMARY KEY,
    id_usuario INT NOT NULL,
    token_hash NVARCHAR(255) NOT NULL,
    jti UNIQUEIDENTIFIER NOT NULL DEFAULT NEWID(),
    exp DATETIME2 NOT NULL,
    revoked_at DATETIME2 NULL,
    user_agent NVARCHAR(200) NULL,
    ip NVARCHAR(64) NULL,
    created_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
  );
  CREATE INDEX IX_RefreshToken_user ON dbo.RefreshToken(id_usuario, exp);
END;

IF OBJECT_ID('dbo.PasswordReset','U') IS NULL
BEGIN
  CREATE TABLE dbo.PasswordReset(
    id INT IDENTITY(1,1) PRIMARY KEY,
    id_usuario INT NOT NULL,
    reset_token_hash NVARCHAR(255) NOT NULL,
    exp DATETIME2 NOT NULL,
    used_at DATETIME2 NULL,
    created_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
  );
  CREATE INDEX IX_PasswordReset_user ON dbo.PasswordReset(id_usuario, exp);
END;
`);
}
ensureAuthTables().catch(e => console.error('ensureAuthTables', e));

// Utils
function sha256(s) { return require('crypto').createHash('sha256').update(String(s), 'utf8').digest('hex'); }
function daysFromNow(days) { const d = new Date(); d.setUTCDate(d.getUTCDate()+days); return d; }
function bearer(req) {
  const h = req.headers.authorization || '';
  const m = h.match(/^Bearer\s+(.+)$/i);
  return m ? m[1] : null;
}
function signAccess(u) {
  return jwt.sign({
    sub: u.id_usuario, usr: u.nombre_usuario,
    mfa_enabled: !!u.mfa_enabled, mfa_method: u.mfa_primary_method || null
  }, ACCESS_TOKEN_SECRET, { expiresIn: ACCESS_TOKEN_TTL });
}
async function issueRefresh(id_usuario, userAgent, ip) {
  const token = jwt.sign({ sub: id_usuario }, REFRESH_TOKEN_SECRET, { expiresIn: `${REFRESH_TOKEN_TTL_DAYS}d` });
  const decoded = jwt.decode(token);
  const exp = new Date(decoded.exp * 1000);
  const hash = sha256(token);
  const p = await getPool();
  await p.request()
    .input('uid', id_usuario)
    .input('h', hash)
    .input('exp', exp)
    .input('ua', userAgent || null)
    .input('ip', ip || null)
    .query(`
      INSERT INTO dbo.RefreshToken(id_usuario, token_hash, exp, user_agent, ip)
      VALUES (@uid, @h, @exp, @ua, @ip);
    `);
  return token;
}
async function revokeRefresh(token) {
  if (!token) return;
  const p = await getPool();
  await p.request().input('h', sha256(token)).query(`
    UPDATE dbo.RefreshToken SET revoked_at = SYSUTCDATETIME()
    WHERE token_hash = @h AND revoked_at IS NULL;
  `);
}
async function isRefreshValid(token) {
  try {
    const payload = jwt.verify(token, REFRESH_TOKEN_SECRET);
    const p = await getPool();
    const r = await p.request().input('h', sha256(token)).query(`
      SELECT TOP 1 * FROM dbo.RefreshToken WHERE token_hash = @h AND revoked_at IS NULL AND exp > SYSUTCDATETIME();
    `);
    if (!r.recordset.length) return null;
    return payload; // { sub, iat, exp }
  } catch { return null; }
}

// Middleware auth por access token
async function authRequired(req, res, next) {
  try {
    const token = bearer(req);
    if (!token) return res.status(401).json({ error: 'NO_TOKEN' });
    req.user = jwt.verify(token, ACCESS_TOKEN_SECRET);
    next();
  } catch {
    return res.status(401).json({ error: 'TOKEN_INVALID' });
  }
}

// --------------------------- Autenticación ---------------------------

// 1) POST /auth/register (protegido por ADMIN_REGISTER_SECRET simple)
app.post('/auth/register', async (req, res) => {
  try {
    if ((req.headers['x-admin-secret'] || '') !== ADMIN_REGISTER_SECRET) {
      return res.status(403).json({ error: 'FORBIDDEN' });
    }
    const { nombre_usuario, contrasena, email } = req.body || {};
    if (!nombre_usuario || !contrasena) return res.status(400).json({ error: 'MISSING_FIELDS' });
    const hash = await bcrypt.hash(contrasena, 12);
    const p = await getPool();
    const r = await p.request()
      .input('u', nombre_usuario)
      .input('h', hash)
      .input('e', email || null)
      .query(`
        INSERT INTO dbo.Usuario(nombre_usuario, contrasena_hash, email)
        OUTPUT INSERTED.id_usuario, INSERTED.nombre_usuario, INSERTED.email
        VALUES (@u, @h, @e);
      `);
    res.status(201).json(r.recordset[0]);
  } catch (err) {
    if ((err.number === 2627 || err.number === 2601)) {
      return res.status(409).json({ error: 'USERNAME_TAKEN' });
    }
    console.error('POST /auth/register', err);
    res.status(500).json({ error: 'DB_ERROR', message: err.message });
  }
});

// 2) POST /auth/login (fase 1)
app.post('/auth/login', async (req, res) => {
  try {
    const { nombre_usuario, contrasena } = req.body || {};
    if (!nombre_usuario || !contrasena) return res.status(400).json({ error: 'MISSING_FIELDS' });
    const p = await getPool();
    const r = await p.request().input('u', nombre_usuario).query(`SELECT TOP 1 * FROM dbo.Usuario WHERE nombre_usuario = @u;`);
    if (!r.recordset.length) return res.status(401).json({ error: 'INVALID_CREDENTIALS' });
    const u = r.recordset[0];

    // Lockout simple
    if (u.mfa_locked_until && new Date(u.mfa_locked_until) > new Date()) {
      return res.status(423).json({ error: 'LOCKED', until: u.mfa_locked_until });
    }

    const ok = await bcrypt.compare(contrasena, u.contrasena_hash);
    if (!ok) {
      await p.request().input('id', u.id_usuario).query(`
        UPDATE dbo.Usuario SET mfa_failed_count = mfa_failed_count + 1,
          mfa_locked_until = CASE WHEN mfa_failed_count + 1 >= 5 THEN DATEADD(minute, 15, SYSUTCDATETIME()) ELSE mfa_locked_until END
        WHERE id_usuario = @id;
      `);
      return res.status(401).json({ error: 'INVALID_CREDENTIALS' });
    }

    // reset contador
    await p.request().input('id', u.id_usuario).query(`
      UPDATE dbo.Usuario SET mfa_failed_count = 0, mfa_locked_until = NULL WHERE id_usuario = @id;
    `);

    if (u.mfa_enabled) {
      const challenge = jwt.sign({ sub: u.id_usuario, purpose: 'mfa', usr: u.nombre_usuario }, MFA_CHALLENGE_SECRET, { expiresIn: '5m' });
      return res.json({ need_mfa: true, login_challenge: challenge, methods: [u.mfa_primary_method || 'TOTP'] });
    }

    // Sin MFA: emitir tokens
    const access = signAccess(u);
    const refresh = await issueRefresh(u.id_usuario, req.headers['user-agent'], req.ip);
    res.json({ need_mfa: false, access_token: access, refresh_token: refresh });
  } catch (err) {
    console.error('POST /auth/login', err);
    res.status(500).json({ error: 'DB_ERROR', message: err.message });
  }
});
//Probé hasta aquí
// 3) POST /auth/mfa/verify (fase 2)
app.post('/auth/mfa/verify', async (req, res) => {
  try {
    const { login_challenge, code } = req.body || {};
    if (!login_challenge || !code) return res.status(400).json({ error: 'MISSING_FIELDS' });
    let payload;
    try {
      payload = jwt.verify(login_challenge, MFA_CHALLENGE_SECRET);
    } catch {
      return res.status(401).json({ error: 'CHALLENGE_INVALID' });
    }
    const p = await getPool();
    const r = await p.request().input('id', payload.sub).query(`SELECT TOP 1 * FROM dbo.Usuario WHERE id_usuario = @id;`);
    if (!r.recordset.length) return res.status(404).json({ error: 'NOT_FOUND' });
    const u = r.recordset[0];
    if (!u.mfa_enabled) return res.status(409).json({ error: 'MFA_NOT_ENABLED' });

    // Verificar TOTP con secreto guardado (VARBINARY -> base32 se guardó como binario de la cadena base32)
    const secretBase32 = Buffer.isBuffer(u.mfa_totp_secret) ? Buffer.from(u.mfa_totp_secret).toString('utf8') : (u.mfa_totp_secret || '');
    const verified = speakeasy.totp.verify({ secret: secretBase32, encoding: 'base32', token: String(code), window: 1 });
    if (!verified) {
      await p.request().input('id', u.id_usuario).query(`UPDATE dbo.Usuario SET mfa_failed_count = mfa_failed_count + 1 WHERE id_usuario = @id;`);
      return res.status(401).json({ error: 'MFA_INVALID' });
    }

    // OK → emitir tokens y resetear contador
    await p.request().input('id', u.id_usuario).query(`UPDATE dbo.Usuario SET mfa_failed_count = 0 WHERE id_usuario = @id;`);
    const access = signAccess(u);
    const refresh = await issueRefresh(u.id_usuario, req.headers['user-agent'], req.ip);
    res.json({ access_token: access, refresh_token: refresh });
  } catch (err) {
    console.error('POST /auth/mfa/verify', err);
    res.status(500).json({ error: 'DB_ERROR', message: err.message });
  }
});

// 4) POST /auth/refresh (rotativo)
app.post('/auth/refresh', async (req, res) => {
  try {
    const { refresh_token } = req.body || {};
    if (!refresh_token) return res.status(400).json({ error: 'MISSING_REFRESH' });
    const payload = await isRefreshValid(refresh_token);
    if (!payload) return res.status(401).json({ error: 'REFRESH_INVALID' });

    // Rotar: revocar viejo y emitir nuevo
    await revokeRefresh(refresh_token);

    const p = await getPool();
    const r = await p.request().input('id', payload.sub).query(`SELECT TOP 1 * FROM dbo.Usuario WHERE id_usuario = @id;`);
    if (!r.recordset.length) return res.status(404).json({ error: 'NOT_FOUND' });
    const u = r.recordset[0];

    const access = signAccess(u);
    const refresh = await issueRefresh(u.id_usuario, req.headers['user-agent'], req.ip);
    res.json({ access_token: access, refresh_token: refresh });
  } catch (err) {
    console.error('POST /auth/refresh', err);
    res.status(500).json({ error: 'DB_ERROR', message: err.message });
  }
});

// 5) POST /auth/logout
app.post('/auth/logout', async (req, res) => {
  try {
    const { refresh_token } = req.body || {};
    if (refresh_token) await revokeRefresh(refresh_token);
    res.json({ ok: true });
  } catch (err) {
    console.error('POST /auth/logout', err);
    res.status(500).json({ error: 'DB_ERROR', message: err.message });
  }
});

// 6) GET /auth/me
app.get('/auth/me', authRequired, async (req, res) => {
  try {
    const p = await getPool();
    const r = await p.request().input('id', req.user.sub).query(`
      SELECT id_usuario, nombre_usuario, email, mfa_enabled, mfa_primary_method
      FROM dbo.Usuario WHERE id_usuario = @id;
    `);
    if (!r.recordset.length) return res.status(404).json({ error: 'NOT_FOUND' });
    res.json(r.recordset[0]);
  } catch (err) {
    console.error('GET /auth/me', err);
    res.status(500).json({ error: 'DB_ERROR', message: err.message });
  }
});

// 7) POST /auth/mfa/setup (genera secreto TOTP provisional)  [requiere login]
app.post('/auth/mfa/setup', authRequired, async (req, res) => {
  try {
    const secret = speakeasy.generateSecret({ length: 20, name: `MedPresc (${req.user.usr})` }); // .base32
    const p = await getPool();
    await p.request()
      .input('id', req.user.sub)
      .input('sec', Buffer.from(secret.base32, 'utf8'))
      .query(`UPDATE dbo.Usuario SET mfa_totp_secret = @sec WHERE id_usuario = @id;`);
    res.json({ otpauth_url: secret.otpauth_url, base32: secret.base32 });
  } catch (err) {
    console.error('POST /auth/mfa/setup', err);
    res.status(500).json({ error: 'DB_ERROR', message: err.message });
  }
});

// 8) POST /auth/mfa/enable (confirma TOTP y habilita)
app.post('/auth/mfa/enable', authRequired, async (req, res) => {
  try {
    const { code } = req.body || {};
    if (!code) return res.status(400).json({ error: 'MISSING_CODE' });
    const p = await getPool();
    const r = await p.request().input('id', req.user.sub).query(`SELECT TOP 1 * FROM dbo.Usuario WHERE id_usuario = @id;`);
    if (!r.recordset.length) return res.status(404).json({ error: 'NOT_FOUND' });
    const u = r.recordset[0];
    const secretBase32 = Buffer.isBuffer(u.mfa_totp_secret) ? Buffer.from(u.mfa_totp_secret).toString('utf8') : (u.mfa_totp_secret || '');
    if (!secretBase32) return res.status(400).json({ error: 'NO_SECRET' });

    const ok = speakeasy.totp.verify({ secret: secretBase32, encoding: 'base32', token: String(code), window: 1 });
    if (!ok) return res.status(401).json({ error: 'MFA_INVALID' });

    // (Opcional) generar recovery codes y guardar hashes JSON
    const rec = Array.from({ length: 8 }, () => require('crypto').randomBytes(5).toString('hex'));
    const recHashes = rec.map(x => sha256(x));
    await p.request()
      .input('id', req.user.sub)
      .input('meth', 'TOTP')
      .input('rec', JSON.stringify(recHashes))
      .query(`
        UPDATE dbo.Usuario
        SET mfa_enabled = 1, mfa_primary_method = @meth, mfa_recovery_codes_hash = @rec
        WHERE id_usuario = @id;
      `);
    res.json({ ok: true, recovery_codes: rec }); // <— muéstralos **una sola vez** al cliente
  } catch (err) {
    console.error('POST /auth/mfa/enable', err);
    res.status(500).json({ error: 'DB_ERROR', message: err.message });
  }
});

// 9) POST /auth/password/forgot  (emite token de reset — dev: lo devuelve para probar)
app.post('/auth/password/forgot', async (req, res) => {
  try {
    const { nombre_usuario } = req.body || {};
    if (!nombre_usuario) return res.status(400).json({ error: 'MISSING_USER' });
    const p = await getPool();
    const r = await p.request().input('u', nombre_usuario).query(`SELECT TOP 1 id_usuario, email FROM dbo.Usuario WHERE nombre_usuario = @u;`);
    if (!r.recordset.length) return res.status(404).json({ error: 'NOT_FOUND' });
    const u = r.recordset[0];

    const raw = require('crypto').randomBytes(24).toString('hex');
    const hash = sha256(raw);
    const exp = daysFromNow(1); // 24h
    await p.request().input('uid', u.id_usuario).input('h', hash).input('exp', exp).query(`
      INSERT INTO dbo.PasswordReset(id_usuario, reset_token_hash, exp) VALUES (@uid, @h, @exp);
    `);
    // En producción: enviar por email. Para el reto: lo retornamos para tests.
    res.json({ ok: true, reset_token_dev: raw, exp });
  } catch (err) {
    console.error('POST /auth/password/forgot', err);
    res.status(500).json({ error: 'DB_ERROR', message: err.message });
  }
});

// 10) POST /auth/password/reset
app.post('/auth/password/reset', async (req, res) => {
  try {
    const { nombre_usuario, reset_token, nueva_contrasena } = req.body || {};
    if (!nombre_usuario || !reset_token || !nueva_contrasena) return res.status(400).json({ error: 'MISSING_FIELDS' });
    const p = await getPool();
    const u = await p.request().input('u', nombre_usuario).query(`SELECT TOP 1 id_usuario FROM dbo.Usuario WHERE nombre_usuario = @u;`);
    if (!u.recordset.length) return res.status(404).json({ error: 'NOT_FOUND' });
    const uid = u.recordset[0].id_usuario;

    const h = sha256(reset_token);
    const t = await p.request().input('uid', uid).input('h', h).query(`
      SELECT TOP 1 * FROM dbo.PasswordReset
      WHERE id_usuario = @uid AND reset_token_hash = @h AND used_at IS NULL AND exp > SYSUTCDATETIME()
      ORDER BY id DESC;
    `);
    if (!t.recordset.length) return res.status(400).json({ error: 'TOKEN_INVALID' });

    const hash = await bcrypt.hash(nueva_contrasena, 12);
    await p.request().input('uid', uid).input('h', hash).query(`
      UPDATE dbo.Usuario SET contrasena_hash = @h WHERE id_usuario = @uid;
    `);
    await p.request().input('id', t.recordset[0].id).query(`UPDATE dbo.PasswordReset SET used_at = SYSUTCDATETIME() WHERE id = @id;`);

    // Opcional: revocar refresh activos del usuario
    await p.request().input('uid', uid).query(`
      UPDATE dbo.RefreshToken SET revoked_at = SYSUTCDATETIME() WHERE id_usuario = @uid AND revoked_at IS NULL;
    `);

    res.json({ ok: true });
  } catch (err) {
    console.error('POST /auth/password/reset', err);
    res.status(500).json({ error: 'DB_ERROR', message: err.message });
  }
});

// 11) DELETE /admin/users/:id  (borrar usuario) — protegido por X-Admin-Secret
app.delete('/admin/users/:id', async (req, res) => {
  try {
    if ((req.headers['x-admin-secret'] || '') !== ADMIN_REGISTER_SECRET) {
      return res.status(403).json({ error: 'FORBIDDEN' });
    }
    const p = await getPool();

    // Revocar refresh tokens
    await p.request().input('uid', parseInt(req.params.id,10)).query(`
      UPDATE dbo.RefreshToken SET revoked_at = SYSUTCDATETIME() WHERE id_usuario = @uid AND revoked_at IS NULL;
    `);

    const r = await p.request().input('id', parseInt(req.params.id,10)).query(`
      DELETE FROM dbo.Usuario OUTPUT DELETED.id_usuario, DELETED.nombre_usuario WHERE id_usuario = @id;
    `);
    if (!r.recordset.length) return res.status(404).json({ error: 'NOT_FOUND' });
    res.json({ ok: true, deleted: r.recordset[0] });
  } catch (err) {
    console.error('DELETE /admin/users/:id', err);
    res.status(500).json({ error: 'DB_ERROR', message: err.message });
  }
});

//este lo probe y en teoria funciona, pero solo probe con el password, los codigos no se todavia pq si tengo que activar de nuevo el mfa me voy a eplota
// 12) POST /auth/mfa/disable
app.post('/auth/mfa/disable', authRequired, async (req, res) => {
  try {
    const { code, recovery_code, password } = req.body || {};
    if (!code && !recovery_code && !password)
      return res.status(400).json({ error: 'MISSING_VERIFICATION' });

    const p = await getPool();
    const r = await p.request().input('id', req.user.sub)
      .query(`SELECT TOP 1 * FROM dbo.Usuario WHERE id_usuario = @id;`);
    if (!r.recordset.length) return res.status(404).json({ error: 'NOT_FOUND' });

    const u = r.recordset[0];
    if (!u.mfa_enabled) return res.status(409).json({ error: 'MFA_NOT_ENABLED' });

    let verified = false, recoveryUsed = false;

    if (!verified && password) {
      try { if (await bcrypt.compare(String(password), u.contrasena_hash)) verified = true; } catch {}
    }

    if (!verified && code) {
      const secretBase32 = Buffer.isBuffer(u.mfa_totp_secret)
        ? Buffer.from(u.mfa_totp_secret).toString('utf8')
        : (u.mfa_totp_secret || '');
      if (secretBase32) {
        const ok = speakeasy.totp.verify({
          secret: secretBase32, encoding: 'base32', token: String(code), window: 1
        });
        if (ok) verified = true;
      }
    }

    if (!verified && recovery_code) {
      try {
        const list = u.mfa_recovery_codes_hash ? JSON.parse(u.mfa_recovery_codes_hash) : [];
        const rcHash = sha256(String(recovery_code).trim());
        const idx = list.indexOf(rcHash);
        if (idx !== -1) {
          list.splice(idx, 1);
          await p.request().input('id', u.id_usuario).input('rec', JSON.stringify(list))
            .query(`UPDATE dbo.Usuario SET mfa_recovery_codes_hash = @rec WHERE id_usuario = @id;`);
          verified = true; recoveryUsed = true;
        }
      } catch {}
    }

    if (!verified) return res.status(401).json({ error: 'VERIFY_FAILED' });

    await p.request().input('id', u.id_usuario).query(`
      UPDATE dbo.Usuario
      SET mfa_enabled = 0,
          mfa_primary_method = NULL,
          mfa_totp_secret = NULL,
          mfa_webauthn_credential = NULL,
          mfa_recovery_codes_hash = NULL,
          mfa_failed_count = 0,
          mfa_locked_until = NULL
      WHERE id_usuario = @id;
    `);

    res.json({ ok: true, recovery_code_used: recoveryUsed });
  } catch (err) {
    console.error('POST /auth/mfa/disable', err);
    res.status(500).json({ error: 'DB_ERROR', message: err.message });
  }
});

const port = process.env.PORT || 3000;
app.listen(port, () => console.log(`Listening on port ${port}...`));