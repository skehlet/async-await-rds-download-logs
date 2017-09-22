const Promise = require('bluebird');
const AWS = require('aws-sdk');
AWS.config.update({region:'us-west-2'});
const rds = new AWS.RDS();
const _ = require('lodash');
const fs = Promise.promisifyAll(require('fs'));
const path = Promise.promisifyAll(require('path'));
const rimraf = Promise.promisify(require('rimraf'));
const {exec} = require('child_process');
const execAsync = Promise.promisify(exec);

const WorkDir = `${__dirname}/work`;
const DBInstanceIdentifier = process.env.DBInstanceIdentifier;

if (!DBInstanceIdentifier) {
    console.error('Set the DBInstanceIdentifier environment variable');
    process.exit(1);
}

process.chdir(__dirname);

async function main() {
    const data = await rds.describeDBLogFiles({
        DBInstanceIdentifier,
        MaxRecords: 0
    }).promise();
    const today12am = getTimestampTodayAt12am() * 1000; // aws uses milliseconds
    const todaysLogs = data.DescribeDBLogFiles.filter((log) => log.LastWritten >= today12am);
    setupOutputDir();
    for (let i = 0; i < todaysLogs.length; i++) {
        await downloadDBLog(todaysLogs[i].LogFileName);
    }
    runPgbadger();
}

try {
    main();
} catch (err) {
    console.log(err);
}


function getTimestampTodayAt12am() {
    var now = new Date();
    var startOfDay = new Date(now.getFullYear(), now.getMonth(), now.getDate());
    var timestamp = startOfDay / 1000;
    return timestamp;    
}

async function setupOutputDir() {
    await rimraf(WorkDir);
    await fs.mkdirAsync(WorkDir);
}

async function downloadDBLog(fileName) {
    const outputFile = `${WorkDir}/${path.basename(fileName)}`;
    try {
        await fs.unlinkAsync(outputFile);
    } catch (err) {
        // ignore
    }
    let nextMarker = null;
    let isTruncated = false;
    do {
        const data = await rds.downloadDBLogFilePortion({
            DBInstanceIdentifier,
            LogFileName: fileName,
            Marker: nextMarker
        }).promise();
        await fs.appendFileAsync(outputFile, data.LogFileData);
        console.log('Wrote', data.LogFileData.length, 'chars to', outputFile);
        isTruncated = data.AdditionalDataPending;
        if (isTruncated) {
            nextMarker = data.Marker;
        }
    } while (isTruncated);
}

async function runPgbadger() {
    try {
        process.chdir(WorkDir);
        console.log(await shellCommand("pgbadger -p '%t:%r:%u@%d:[%p]:' *"));
    } catch (err) {
        console.log('pgbadger err:', err);
    } finally {
        process.chdir(__dirname);
    }
}

async function shellCommand(shellCmd) {
    let output = await execAsync(`${shellCmd} 2>&1`);
    return output.trimRight();
}
