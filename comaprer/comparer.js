const path = require('path');
const fs = require('fs');
const _ = require('lodash');

const argv = require('yargs')
    .usage('Usage: $0 -a accuratePercentileFile -t tdigestPercentileFile -d allowedDeviation [-o outFile]')
    .example('-a accPer.json -t tdPer.json -d 1 -o ./output/compare.json')
    .describe('awsConfigPath', 'AWS configuration json file')
    .describe('a', 'Accurate percentile file path')
    .alias('a', 'accuratePercentileFile')
    .describe('t', 'TDigest percentile file path')
    .alias('t', 'tdigestPercentileFile')
    .describe('d', 'Deviation threshold(percent)')
    .alias('d', 'deviationThreshold')
    .describe('o', 'Output file path')
    .alias('o', 'outputFile')
    .demandOption(['a', 't', 'd'])
    .argv;

const accurateData = require(argv.accuratePercentileFile);
const tdigestData = require(argv.tdigestPercentileFile);

let accGrp = _.groupBy(accurateData, item => item.script_id);
let tdGrp = _.groupBy(tdigestData, item => item.scriptId);
let totalTransactions = 0;

_.forOwn(accGrp, (value, key) => {
    accGrp[key] = _.orderBy(value, item => item.name);
});

_.forOwn(tdGrp, (value, key) => {
    tdGrp[key] = _.orderBy(value, item => item.transName);
});

let compareData = {
    scripts: {},
    alerts: [],
    totalAlerts: 0
};

_.forOwn(accGrp, (value, key) => {
    let transactions = [];
    for (let idx = 0; idx < value.length; idx++) {
        let accTrans = value[idx];
        let tdTrans = tdGrp[key][idx];
        let deviation = ((tdTrans.percentile / accTrans.nintieth) * 100) - 100;
        let trans = {
            scriptId: key,
            transName: accTrans.name,
            accurate: accTrans.nintieth,
            tdigest: tdTrans.percentile,
            deviation: deviation,
            alert: deviation < -argv.deviationThreshold || deviation > argv.deviationThreshold
        };
        transactions.push(trans);
        if (trans.alert) {
            compareData.alerts.push(trans);
            compareData.totalAlerts++;
        }
        totalTransactions++;
    }
    compareData.scripts[key] = transactions;
});

if (argv.outputFile) {
    let outputStream = fs.createWriteStream(argv.outputFile);
    outputStream.write(JSON.stringify(compareData));
    outputStream.end();
}

console.log(`Total alerts = ${compareData.totalAlerts} out of Total transactions = ${totalTransactions}; Ratio = ${(compareData.totalAlerts/totalTransactions*100).toFixed(2)}%`);
