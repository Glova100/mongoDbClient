'use strict'

const prompt = require('prompt')
const dbConn = require('./mongodb')
const colors = require('colors/safe')

const parseSql = require('node-sqlparser').parse
let _db 

const green = colors.green
const red = colors.red
const grey = colors.grey


dbConn.then( db => {
    _db = db
    console.log( green("Connected") )

    run(db)
})
.catch( err => {
    console.log( red(err) )
    
})


function run(db) {
    readQuery()
        .then(parseQuery)
        .then( query => {
            if(!query) return null
            
            return execQuery(db, query)
        })
        .then( data => {
            if(data) console.log( grey(JSON.stringify(data, null, 4)) )
            run(db)
        })
        .catch( err => {
            console.log( red(err.message) )
            run(db)
        })
}


function readQuery() {
    return new Promise( (resolve, reject) => {
        prompt.message = '>'
        prompt.start()
        prompt.get(['query'], function (err, result) {
            if (err) return quit()

           
            if(result.query === 'quit') quit()

            resolve(result.query.trim())
        })    
    })
}


function execQuery(db, query) {
    return new Promise( (resolve, reject) => {
        const aggr = []
        const cursor = db.collection(query.collection).find(query.where, {
            limit: query.limit
        }).sort(query.sort)

        cursor.each( (err, doc) => {
            if(err) return reject(err)

            if(!doc) {
                return resolve(aggr)
            }

            if(doc) {
             
                if(query.fields.length > 0) {
                    const o = {}

                    for(const proj of query.fields) {
                        if(doc[proj]) o[proj] = doc[proj]
                    }

                    if(Object.keys(o).length > 0) aggr.push(o)
                } else {
                    
                    aggr.push(doc)
                }
            }
        })
    })
}


const exprMapper = {
    '=': '$eq',
    '<>': '$ne',
    '>': '$gt',
    '<': '$lt',
    '>=': '$ge',
    '<=': '$le',

    'and': '$and',
    'or': '$or',
    
}


function parseWhere(root) {
    if(root.type === 'binary_expr') {
        const operator = root.operator;

       
        if(operator === 'AND') {
            const [left, e1] = parseWhere(root.left)
            const [right, e2] = parseWhere(root.right)

            return { 
                [left]: e1,
                [right]: e2
            }
        } else if(operator === 'OR') {
            const [left, e1] = parseWhere(root.left)
            const [right, e2] = parseWhere(root.right)

            return  { '$or' : [ 
                        { [left]: e1 }, 
                        { [right]: e2 }
                    ]}
        } else {
            const field = root.left.column
            const expr = exprMapper[operator]

            return [ field, { [expr]: root.right.value } ]
        }
    } else return {}
}


function parseQuery(query) {
    return new Promise( (resolve, reject) => {
        if(!query) return resolve()
        const astObj = parseSql(query)
        
       
        
        if(astObj.type != 'select') return reject(new Error('This version only supports \'SELECT\' queries'))
        if(!astObj.from) return reject(new Error('You should specify a collection in \'FROM\' clause'))

        const o = {}
        o.collection = astObj.from[0].table
        o.fields = []
        o.where = {}
        o.sort =  {}
        o.limit = astObj.limit ? parseInt(astObj.limit[1].value) : null

        if(astObj.columns !== '*') {
            o.fields = map(astObj.columns, e => e.expr.column)
        }

        if(astObj.where) {
            
            try {
                let where = parseWhere(astObj.where)

                /
                if(Array.isArray(where)) {
                    where = { [where[0]]: where[1] }
                }

                o.where = where
            } catch(ex) { console.log( red(ex.stack) ) }

            
        }

        if(astObj.orderby) {
            for(const i in astObj.orderby) {
                const elem = astObj.orderby[i]
                o.sort[ elem.expr.column ] = elem.type === 'DESC' ? -1 : 1
            }
        }

        resolve(o)
    })
}


function map(a, cb) {
    const _new = []

    for(const i in a) {
        _new.push(cb(a[i], i, a))
    }

    return _new
}


function quit() {
    if(_db) _db.close()
    prompt.stop()
    process.exit() 
}