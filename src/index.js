const app = require('./app')
const db = require('./db')

async function main() {

    await app.listen(app.get('port'));
    console.log('Server on port', app.get('port'));
}

main();