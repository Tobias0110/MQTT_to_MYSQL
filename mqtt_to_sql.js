import{ argv } from 'node:process';
import mysql from 'mysql';
import mqtt from 'mqtt';
import { readFileSync } from 'node:fs';

const config = JSON.parse( readFileSync('./config.json', 'utf-8' ));

let deb = false;
if(argv[2] == "-d") deb = true;

function current_time() {
    let date = new Date();
    return date.toISOString().slice(0, 19).replace('T', ' ');
}

let con = mysql.createConnection({
    host: config.sql_ip,
    user: config.sql_user,
    password: config.sql_passwd
});

con.connect(function(err) {
    if (err) throw err;
    console.log("[MYSQL] Connected!");
  });

const mqtt_con = mqtt.connect("mqtt://" + config.mqtt_ip, {clientId:"MQTT_to_SQL", username: config.mqtt_user, password: config.mqtt_passwd});

mqtt_con.on("connect", () =>{
    console.log("[MQTT] Connected!");
    for (const x in config.bridges){
        mqtt_con.subscribe(x, (err) => {
            if (err) throw err;
        });
    }
});

mqtt_con.on("message", (topic, message) => {
    let columns = new Array(config.bridges[topic].table);
    let values = new Array();

    if(deb == true) console.log("\n\n\n" + config.bridges[topic].table);

    let mess = JSON.parse(message.toString());

    //sql = "INSERT INTO " + config.bridges[topic].table + " (";
    let sql_columns = "INSERT INTO ?? ("
    let sql_values = ") VALUES (";

    for (const x in config.bridges[topic].links){
        if(deb == true) console.log(config.bridges[topic].links[x].column);
        sql_columns = sql_columns + "??, ";
        sql_values = sql_values + "?, ";
        columns.push(config.bridges[topic].links[x].column);
        if(deb == true) console.log(config.bridges[topic].links[x].type);

        if(config.bridges[topic].links[x].type == "message") {
            if(deb == true) console.log(message.toString());
            values.push(message.toString());
        }
        else if(config.bridges[topic].links[x].type == "current_time") {
            if(deb == true) console.log(current_time());
            values.push(current_time());
        }
        else {
            
            if(deb == true) console.log(mess[x]);
            // integer found_int != null
            if((config.bridges[topic].links[x].type == "int") && (!Number.isNaN(parseInt(mess[x])))) {
            values.push(parseInt(mess[x]));
            }
            // float
            else if((config.bridges[topic].links[x].type == "float") && (!Number.isNaN(parseFloat(mess[x])))) {
            values.push(parseFloat(mess[x]));
            }
            // string
            else if(config.bridges[topic].links[x].type == "str") {
                values.push(mess[x].toString());
                }
            else {
                values.push(null);
            }
        }
    }
    // remove last komma
    let reg = /\,(?:.(?!\,))+$/g;
    sql_columns = sql_columns.replace(reg, "");
    sql_values = sql_values.replace(reg, "");
    let sql_command = sql_columns + sql_values + ");";
    if(deb == true) console.log(sql_command);

    if(deb == true)console.log(columns);
    if(deb == true)console.log(values);

    con.query(sql_command, columns.concat(values), function (err, result, fields) {
        if (err) throw err;
        if(deb == true)console.log(result);
      });

    /*con.query(sql_command, function (err, result, fields) {
        if (err) throw err;
        console.log(result);
      });*/

    /*con.query("SELECT * FROM stromzaehler.stromzaehler_nebi", function (err, result, fields) {
        if (err) throw err;
        console.log(result);
      });*/
  });