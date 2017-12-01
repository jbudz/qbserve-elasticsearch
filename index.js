const path = require('path')
const minimatch = require('minimatch')
const glob = require('glob');
const del = require('del');
const elasticsearch = require('elasticsearch');
const { readFile, unlink, watch } = require('fs');

const config = (() => {
  const fileConfig = yaml.safeLoad(fs.readFileSync(path.join(process.cwd(), '/config.yml'), 'utf8'))
  return function (objPath) {
    return _.get(fileConfig, objPath);
  }
})();

const EXPORT_PATTERN = '*.json';
const DATA_FOLDER = path.resolve(__dirname, 'exports');

const username = config('es_username');
const password = config('es_password');
const client = new elasticsearch.Client({
  host: [{
    host: config('es_host'),
    port: config('es_port'),
    protocol: config('es_protocol'),
    auth: username && password && `${username}:${password}`
  }]
})

function errorAndExit(error) {
  console.error(e);
  process.exit(1);
}

function secondsToISO(s) {
  return new Date(s * 1000).toISOString();
}

function cleanData(data) {
  const startTime = data.info.start_time;
  const endTime = data.info.end_time;

  const distractingDurationInSeconds = data.totals.distracting_duration;
  const neutralDurationInSeconds = data.totals.neutral_duration;
  const productiveDurationInSeconds = data.totals.productive_duration;

  const totalDurationInSeconds = endTime - startTime;
  const activeDurationInSeconds =
    distractingDurationInSeconds +
    neutralDurationInSeconds +
    productiveDurationInSeconds;

  const activeRatio = activeDurationInSeconds / totalDurationInSeconds;
  const productiveRatio = productiveDurationInSeconds / activeDurationInSeconds;

  return {
    '@timestamp': secondsToISO(startTime),
    start_time: secondsToISO(startTime),
    end_time: secondsToISO(endTime),
    distracted_in_seconds: distractingDurationInSeconds,
    neutral_in_seconds: neutralDurationInSeconds,
    productive_in_seconds: productiveDurationInSeconds,
    active_in_seconds: activeDurationInSeconds,
    total_in_seconds: totalDurationInSeconds,
    active_ratio: activeRatio,
    productive_ratio: productiveRatio
  }
}

function parseData(filePath) {
  return new Promise((resolve, reject) => {
    readFile(filePath, (error, data) => {
      if (error && error.code === 'ENOENT') return resolve(null);
      if (error) return reject(error);

      const parsedData = JSON.parse(data);
      const cleanedData = cleanData(parsedData);
      resolve(cleanedData);
    })
  });
}

function watchForExports(cb) {
  watch(DATA_FOLDER, async (eventType, filename) => {
    const isDataFile = minimatch(filename, EXPORT_PATTERN);
    if (!isDataFile) return;

    const absolutePath = path.resolve(DATA_FOLDER, filename);
    try {
      const result = await parseData(absolutePath);
      if (result) cb(null, {
        data: result,
        absolutePath
      });
    } catch(e) {
      cb(e)
    }

  })
}

function getISODate() {
  return new Date().toISOString().substring(0, 10);
}

function getIndex(prefix = 'qbserve-') {
  return `${prefix}${getISODate().substring(0, 7).replace('-', '.')}`
}

function getBulkIndexAction({ id }) {
  return {
    index: {
      _index: getIndex(),
      _type: 'doc',
      _id: id
    }
  }
}

function index(metrics) {
  const body = []
  metrics.forEach(metric => {
    const epochTimestamp = new Date(metric['@timestamp']).getTime();
    const action = getBulkIndexAction({
      id: `computer:${epochTimestamp}`
    });
    body.push(action, metric)
  })

  return client.bulk({
    body
  })
}

function getExports() {
  return new Promise((resolve, reject) => {
    const globPath = path.join(DATA_FOLDER, EXPORT_PATTERN);
    glob(globPath, {
      absolute: true
    }, async (err, files) => {
      if (err) return reject(err);
      const bulkData = await Promise.all(
        files.map(parseData)
      );

      resolve({
        bulkData,
        files
      })
    });
  })
}

async function indexCurrentExports() {
  try {
    const { bulkData, files } = await getExports();
    if (!bulkData.length) {
      console.log('No waiting files')
      return;
    }

    const response = await index(bulkData);
    if (response.errors) throw response;
    console.log(JSON.stringify(response));

    del(files, err => err && errorAndExit(err));
    console.log(`Removed ${files.join(', ')}`);
  } catch(e) {
    console.log(e);
  }
}


function run() {
  indexCurrentExports();

  watchForExports(async (err, { data, absolutePath}) => {
    if (err) errorAndExit(err)
    try {
      const response = await index([data]);
      if (response.errors) throw response;
      console.log(JSON.stringify(response));

      del(absolutePath, err => err && errorAndExit(err))
      console.log(`Removed ${absolutePath}`);
    } catch(e) {
      errorAndExit(e);
    }
  })
}

run();
