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
        res.send('TABLE DOES NOT EXISTS!');
        next(error);
    }
});

router.delete('/eliminarTemporal', async (req, res, next) => {
    try {
        let result = await p1db.dropTemporal();
        res.setHeader('Content-Type', 'application/json');
        res.statusCode = 200;
        res.json(result);
    } catch(e) {
        var error = new Error("Table is already dropped.")
        res.statusCode = 403;
        res.send(error);
        next(error);    
    }
});

router.get('/cargarModelo', async (req, res, next) => {
    try {
        let result = await p1db.setModel();
        res.setHeader('Content-Type', 'application/json');
        res.statusCode = 200;
        res.json(result);
    } catch(e) {
        var error = new Error(e)
        res.statusCode = 403;
        res.send('TABLE ALREADY EXISTS!')
        next(error);    
    }
});

router.get('/cargarTemporal', async (req, res, next) => {
    try {
        let result = await p1db.setData();
        res.setHeader('Content-Type', 'application/json');
        res.statusCode = 200;
        res.json("DATA LOADED SUCCESSFULLY!");
    } catch(e) {
        var error = new Error(e)
        res.statusCode = 403;
        next(error);    
    }
});

router.delete('/eliminarModelo', async (req, res, next) => {
    try {
        let result = await p1db.dropModel();
        res.setHeader('Content-Type', 'application/json');
        res.statusCode = 200;
        res.json(result);
    } catch(e) {
        var error = new Error(e)
        res.statusCode = 403;
        next(error);    
    }
});


router.get('/consulta1', async (req, res, next) => {
    try {
        let result = await p1db.query1();
        res.setHeader('Content-Type', 'application/json');
        res.statusCode = 200;
        res.json(result);
    } catch(e) {
        var error = new Error(e)
        res.statusCode = 403;
        next(error);    
    }
});

router.get('/consulta2', async (req, res, next) => {
    try {
        let result = await p1db.query2();
        res.setHeader('Content-Type', 'application/json');
        res.statusCode = 200;
        res.json(result);
    } catch(e) {
        var error = new Error(e)
        res.statusCode = 403;
        next(error);    
    }
});

router.get('/consulta3', async (req, res, next) => {
    try {
        let result = await p1db.query3();
        res.setHeader('Content-Type', 'application/json');
        res.statusCode = 200;
        res.json(result);
    } catch(e) {
        var error = new Error(e)
        res.statusCode = 403;
        next(error);    
    }
});

router.get('/consulta4', async (req, res, next) => {
    try {
        let result = await p1db.query4();
        res.setHeader('Content-Type', 'application/json');
        res.statusCode = 200;
        res.json(result);
    } catch(e) {
        var error = new Error(e)
        res.statusCode = 403;
        next(error);    
    }
});

router.get('/consulta5', async (req, res, next) => {
    try {
        let result = await p1db.query5();
        res.setHeader('Content-Type', 'application/json');
        res.statusCode = 200;
        res.json(result);
    } catch(e) {
        var error = new Error(e)
        res.statusCode = 403;
        next(error);    
    }
});

router.get('/consulta6', async (req, res, next) => {
    try {
        let result = await p1db.query6();
        res.setHeader('Content-Type', 'application/json');
        res.statusCode = 200;
        res.json(result);
    } catch(e) {
        var error = new Error(e)
        res.statusCode = 403;
        next(error);    
    }
});

router.get('/consulta7', async (req, res, next) => {
    try {
        let result = await p1db.query7();
        res.setHeader('Content-Type', 'application/json');
        res.statusCode = 200;
        res.json(result);
    } catch(e) {
        var error = new Error(e)
        res.statusCode = 403;
        next(error);    
    }
});

router.get('/consulta8', async (req, res, next) => {
    try {
        let result = await p1db.query8();
        res.setHeader('Content-Type', 'application/json');
        res.statusCode = 200;
        res.json(result);
    } catch(e) {
        var error = new Error(e)
        res.statusCode = 403;
        next(error);    
    }
});

router.get('/consulta9', async (req, res, next) => {
    try {
        let result = await p1db.query9();
        res.setHeader('Content-Type', 'application/json');
        res.statusCode = 200;
        res.json(result);
    } catch(e) {
        var error = new Error(e)
        res.statusCode = 403;
        next(error);    
    }
});

router.get('/consulta10', async (req, res, next) => {
    try { 
        let result = await p1db.query10();
        res.setHeader('Content-Type', 'application/json');
        res.statusCode = 200;
        res.json(result);
    } catch(e) {
        var error = new Error(e)
        res.statusCode = 403;
        next(error);    
    }
});

module.exports = router;