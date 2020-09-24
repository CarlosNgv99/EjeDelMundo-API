const express = require('express');
const p1db = require('../db/index')

const router = express.Router();

router.get('/', async (req, res, next) => {
    try{
        let results = await p1db.getData();
        res.setHeader('Content-Type', 'application/json');
        res.statusCode = 200;
        res.json(results);
    } catch(e) {
        var error = new Error(e)
        res.statusCode = 403;
        next(error);
    }
});

router.get('/cargarModelo', async (req, res, next) => {
    res.send('CargarModelo')
});

module.exports = router;