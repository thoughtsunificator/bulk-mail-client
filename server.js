const path = require('path')
const fs = require('fs').promises
const csv = require('async-csv')
const createCsvWriter = require('csv-writer').createObjectCsvWriter
const nodemailer = require("nodemailer")
const Email = require('email-templates')
const logger = require("tracer").dailyfile({
		root: "./logs",
		maxLogFiles: 10,
		allLogsFileName: "bulk-mail-client"
})
const emailValidator = require("email-validator");
const prettyMilliseconds = require('pretty-ms')
require('console-stamp')(console, '[HH:MM:ss.l]');

const _config = require("./config.json")

const _transporter = nodemailer.createTransport(_config.transport)
let _csvIndex = 0
let _mailCount = 0
let _hourMailCount = 0
const _csv = []
let _interval
let _retryCount = 0

/**
 * @param  {object} csv_ [description]
 * @param  {object} row  [description]
 */
async function sendMail(csv_, row) {
	console.log(`Sending a mail to ${row.email} (${csv_.rows.indexOf(row) + 1}/${csv_.rows.length} (${row.email}))`)
	logger.log("Sending a mail to " + row.email)
	try {
		const info = await csv_.email
			.send({
				template: csv_.template || _config.template,
				message: {
					to: row.email
				},
				locals: {
					...csv_.locals
				}
			})
		row.bulkMailSent = 1
		console.log("Updating CSV...")
		await createCsvWriter({
				path: csv_.path,
				header: Object.keys(row).map(key => ({id: key, title: key}))
		}).writeRecords(csv_.rows)
		csv_.cursor++
		_hourMailCount++
		_mailCount++
		if(csv_.email.preview) {
			console.log("Preview URL: %s", nodemailer.getTestMessageUrl(info))
		}
		console.log(`Message ${info.messageId} sent to ${row.email}`)
		logger.log(`Message ${info.messageId} sent to ${row.email}`)
		if(_config.rate - _hourMailCount >= 1) {
			console.log(`${_config.rate - _hourMailCount} mail(s) left before reaching the limit\n`)
		} else {
			console.log("Rate limit reached.\n")
		}
		console.log(`${_mailCount}/${_csv.map(csv__ => csv__.rows.length).reduce((accumulator, previousValue) => accumulator + previousValue)} mail(s) sent\n`)
		if(_config.rate - _hourMailCount >= 1 && !(csv_.cursor === csv_.rows.length && _csvIndex === _csv.length - 1)) {
			console.log(`Pausing for ${_config.pause_timeout}ms...`)
			return new Promise(resolve => setTimeout(async () => resolve(), _config.pause_timeout))
		}
	} catch(ex) {
		console.error(ex)
		logger.log(ex.message)
		if(_retryCount < _config.retry_count) {
			console.log(`Retrying in ${prettyMilliseconds(_config.retry_timeout)}...`)
			return new Promise(resolve => {
				setTimeout(async () => {
					console.log("Retrying...")
					_retryCount++
					await sendMail(csv_, row)
					resolve()
				}, _config.retry_timeout)
			})
		} else {
			_retryCount = 0
			row.fail = 1
			csv_.cursor++
			console.log("Updating CSV...")
			await createCsvWriter({
					path: csv_.path,
					header: Object.keys(row).map(key => ({id: key, title: key}))
			}).writeRecords(csv_.rows)
			console.log(`Retry limit reached skipping entry and pausing for ${prettyMilliseconds(_config.pause_timeout)}...`)
			return new Promise(resolve => setTimeout(async () => resolve(), _config.pause_timeout))
		}
	}
}

async function run() {
	clearInterval(_interval)
	const date = new Date()
	if(_config.hours.includes(date.getHours())) {
		console.log("Valid hour: sending mails...")
		const csv_ = _csv[_csvIndex]
		console.log(`\ncsv ${_csvIndex + 1}/${_csv.length} (${csv_.path})`)
		const rateDiff =  _config.rate - _hourMailCount
		let count = 0
		for(let i = csv_.cursor; i < csv_.rows.length && count < rateDiff; i++) {
			await sendMail(csv_, csv_.rows[i])
			count++
		}
		if(_csv.filter(csv_ => csv_.cursor === csv_.rows.length).length === _csv.length) {
			console.log(`Finished: stopping interval...`)
			clearInterval(_interval)
			console.log(`${_mailCount} mails were sent across all csv`)
		} else {
			if(_csv[_csvIndex].cursor === _csv[_csvIndex].rows.length) {
				console.log(`Incrementing "_csvIndex" to ${_csvIndex + 1}...`)
				_csvIndex++
			}
			if(_hourMailCount < _config.rate) {
				console.log("Rate limit not yet reached: skipping interval...")
				run()
			} else {
				console.log(`Rate limit reached: resetting "hourMailCount" and restarting interval (${prettyMilliseconds(_config.interval)})...`)
				_hourMailCount = 0
				_interval = setInterval(run, _config.interval)
			}
		}
	} else {
		console.log(`Invalid hour: will be checking every 5m...`)
		_interval = setInterval(run, 300000)
	}
}

(async function() {
	if(_config.template) {
		console.log(`Global template : ${_config.template}`)
	} else if(_config.csv.filter(_csv => _csv.template).length !== _config.csv.length) {
		throw `You need to specify a template for each of your csv or/and use a global template using the "template" key`
	}
	console.log(`rate: ${_config.rate}`)
	console.log(`Reading and parsing ${_config.csv.length} csv file(s)...`)
	for(const csv_ of _config.csv) {
		const csvString = await fs.readFile(csv_.path, 'utf-8');
		const csvRows = _csv.map(csv__ => csv__.rows).flat()
		const rows = (await csv.parse(csvString, { columns: true })).filter(row => !row.bulkMailSent).filter((row, index, array) => {
			return array.findIndex(row_ => row_.email.trim().toLowerCase() === row.email.trim().toLowerCase()) === index
		}).filter(row => {
			return !csvRows.find(row_ => row_.email.trim().toLowerCase() === row.email.trim().toLowerCase())
		}).filter(row => emailValidator.validate(row.email))
		if(rows.length === 0) {
			console.log(`Skipping ${csv_.path} (no new mails were found)`)
			continue
		}
		_csv.push({
			email: new Email({
				message: {
					from: _config.from,
					...(csv_.attachments && { attachments: csv_.attachments.map(attachment => ({...attachment, path: path.join(__dirname, "resource", attachment.path)})) })
				},
				preview: false,
				send: true, // true in prod
				transport: _transporter
			}),
			path: csv_.path,
			cursor: 0,
			locals: csv_.locals,
			rows
		})
		console.log(`Loaded ${csv_.path} (${rows.length} mails)`)
	}
	if(_csv.length >= 1) {
		run()
	} else {
		console.log(`No CSV files were loaded: exiting...\n`)
	}
})()
