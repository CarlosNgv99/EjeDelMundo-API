const mysql = require('mysql')

const pool = mysql.createPool({
  connectionLimit: 10,
  password: '!Juniode7',
  user: 'root',
  database: 'p1db',
  host: 'localhost',
  port: '3306'
});


let p1db = {};

p1db.getData = () => {
  return new Promise((resolve, reject) => {
    pool.query('SELECT * FROM data_center;', (err, res) => {
      if(err){
        reject(err);
      }
      return resolve(res);
    });
  });
}

module.exports = p1db;