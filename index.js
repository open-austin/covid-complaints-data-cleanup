const csvParse = require('csv-parse/lib/sync')
const csvGenerate = require('csv-generate')
const transform = require('stream-transform')
const _ = require("lodash");
const stringify = require('csv-stringify')

const fs = require('fs').promises;
const { Client, Status } = require("@googlemaps/google-maps-services-js");
const { add } = require('lodash');

const client = new Client({
});

// const url = require('url');
// url.fileURLToPath(url)
// url.pathToFileURL(path)

// list of unique addresses
// map of address to geocode results
// map of place_id to place data
// download and save geocode results
// if there are multiple places that match, download them all but choose the first one
// download and save place results
// loop through each row, add place_id, name, types, websites, phone number, business status, user_ratings_total, rating, 

function sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}

async function writeJSON(path, data) {
    return await fs.writeFile(path, JSON.stringify(data, null, 2));
}

async function readJSON(path, data) {
    try {
        return JSON.parse(await fs.readFile(path));
    }
    catch (err) {
        if (err.message.includes('ENOENT')) {
            return null;
        }
        throw err;
    }
}

async function getPlace(placeID) {
    const path = `data/places/${placeID}.json`;
    const existingData = await readJSON(path);
    if (existingData != null) {
        return existingData;
    }

    console.log(`Querying for ${placeID}`)

    const details = await client.placeDetails({
        params: {
            place_id: placeID,
            key: process.env.GOOGLE_MAPS_API_KEY,
        },
    });

    await sleep(10);

    const result = details.data.result;

    await writeJSON(path, result);

    return result;
}

function base64(string) {
    return Buffer.from(string).toString('base64');
}

async function getGeocode(address) {
    if (address == null || address.trim().length === 0) {
        return null;
    }

    const path = `data/geocode/${base64(address)}.json`;
    const existingData = await readJSON(path);
    if (existingData != null) {
        return existingData[0];
    }
    console.log(`Querying for ${address}`)

    const details = await client.geocode({
        params: {
            address: address,
            bounds: {
                northeast: { lat: 30.620516, lng: -97.949466 },
                southwest: { lat: 29.949317, lng: -97.562198 },
            },
            key: process.env.GOOGLE_MAPS_API_KEY,
        },
    });

    await sleep(10);

    const results = details.data.results;

    await writeJSON(path, results);

    return results[0];
}


async function main() {
    try {
        const complaintsRawCSV = await fs.readFile('complaints.csv');
        const complaintsCSV = csvParse(complaintsRawCSV, {
            columns: true,
            skip_empty_lines: true
        });
        const addresses = _.uniq(complaintsCSV.map(c => c.ADDRESS));

        const addressToGeocode = {};
        const geocodeToPlace = {};
        const addressToPlace = {};

        for (const address of addresses) {
            const geocode = await getGeocode(address);
            addressToGeocode[address] = geocode;
            if (geocode == null) {
                console.error('Could not geocode address', address);
                continue;
            }
            const place = await getPlace(geocode.place_id)
            geocodeToPlace[place] = geocode;
            addressToPlace[address] = place;
        }

        for (complaint of complaintsCSV) {
            const place =  addressToPlace[complaint.ADDRESS];
            if (place != null) {
                complaint.google_place_id = place.place_id;
                complaint.google_url = place.url;
                complaint.google_formatted_address = place.formatted_address;
                complaint.google_user_ratings_total = place.user_ratings_total;
                complaint.google_website = place.website;
                complaint.google_types = place.types;
                complaint.google_rating = place.rating;
                complaint.google_business_status = place.business_status;
            }
        }

        stringify(complaintsCSV, {header: true}, function(err, output){
            fs.writeFile('complaints_augmented.csv', output);
          });

    } catch (err) {
        console.error(err);
    }

}

main();

