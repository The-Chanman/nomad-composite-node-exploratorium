const Nomad = require('nomad-stream')
const moment = require('moment')

const nomad = new Nomad()

let instance = null
const timeThreshold = 3 * 60 * 60 * 1000 // 3 hours
const frequency = 60 * 60 * 1000 // 1 hour
let lastPub = null

const defaultPublishData = {
  bayWaterSensor: {
    salinity: {
      data: "",
      units: "",
      time: "",
    }
  },
  baySideBeaconSensor: {
    Dew_Point: {
      data: "",
      units: "",
      time: "",
    }
  }
}

class DataMaintainer {
  constructor(){
    this.data = defaultPublishData
  }
  setValue(key, value){
    let cleanedKey = this.cleanKey(key)
    if(cleanedKey in this.data){
      this.data[cleanedKey].data = value.data
      this.data[cleanedKey].time = value.time
      this.data[cleanedKey].units = value.units
    } else {
      this.data[cleanedKey] = value
    }
  }
  cleanKey(key){
    let cleanedKey = key.replace(/\s+/, '\x01').split('\x01')[0]
    cleanedKey = cleanedKey.toLowerCase()
    return cleanedKey
  }
  getAll(){
    return this.data
  }
  isAllFilled(){
    return this.data["bayWaterSensor"]["salinity"]["data"] && this.data["bayWaterSensor"]["salinity"]["time"] && this.data["baySideBeaconSensor"]["Dew_Point"]["data"] && this.data["baySideBeaconSensor"]["Dew_Point"]["time"]
  }
  clear(){
    this.data = defaultPublishData
  }
  toString(){
    return JSON.stringify(this.data)
  }
}

function getTime() {
  return new moment()
}

let dataManager = new DataMaintainer()

nomad.prepareToPublish()
  .then((n) =>{
    instance = n
    return instance.publishRoot('hello this is a composite node for the exploratorium')
  })
  .then(() =>{
    lastPub = getTime()
    nomad.subscribe(['QmdmSfLYyyKRdadFPbYv8o89ibU4tpcdFve6dqQwbQ9QhU', 'Qmcd8r4J81uxcLSsrUGMh9D4Rh47UYmPNDCEpuifxuNyNy'], function(message) {
      console.log(message.message)
      messageData = JSON.parse(message.message)
      try{
        dataManager.setValue(messageData.columnNames[5], {data: messageData.data[5], time: messageData.data[0]})
      }
      catch(err){
        console.log("DataMaintainer failed with error of " + err)
      }
      console.log(dataManager.toString())
      let currentTime = getTime()
      let timeSince = currentTime - lastPub
      if (timeSince >= frequency){
        console.log("timeSince >= timeBetween")
        if (dataManager.isAllFilled){
          // publish if everything is full
          console.log("***************************************************************************************")
          console.log(dataManager.getAll())
          console.log("***************************************************************************************")

          instance.publish(dataManager.toString())
            .catch(err => console.log(`Error: ${JSON.stringify(err)}`))
          dataManager.clear()  
          lastPub = currentTime
        }
      }
      if (timeSince >= timeThreshold){
        // publish what we got
        instance.publish(dataManager.toString())
          .catch(err => console.log(`Error: ${JSON.stringify(err)}`))
        console.log("***************************************************************************************")
        console.log(dataManager.getAll())
        console.log("***************************************************************************************")
        dataManager.clear()  
        lastPub = currentTime
      }
    })
  })
  .catch(err => console.log(`Error in main loop: ${JSON.stringify(err)}`))


