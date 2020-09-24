const express = require('express');
const morgan = require('morgan');
const bodyParser = require('body-parser');
const app = express();

const indexRouter = require('./routes/index')

app.use(bodyParser.json());
app.use(morgan('dev'));

app.listen(process.env.PORT || 3000, () => {
    console.log(`Server running on port: ${process.env.PORT || '3000'}`);
});

app.use('/', indexRouter);
