const Nomad = require('nomad-stream')
const moment = require('moment')

const nomad = new Nomad()

let instance = null
const timeThreshold = 1 * 60 * 60 * 1000 // 1 hour
const frequency =  10 * 60 * 1000 // 10 minutes
let lastPub = null
const subscriptions = ['QmW7MrsugXX5nrBtJpGadrZRtT7xTMxuZH9F1A8UEi3kMa', 'QmXeYj4i32SAynbS43jWuJSinVRveZQ7YoMWL9RsfkDE6h']


const defaultPublishData = {

  dissolved_o2: {
    data: "",
    units: "",
    time: "",
    nodeID: "",
  },
  dew_point: {
    data: "",
    units: "",
    time: "",
    nodeID: "",
  }
}

class DataMaintainer {
  constructor(){
    this.data = defaultPublishData
  }
  setValue(key, id, value){
    let cleanedKey = this.cleanKey(key)
    if(cleanedKey in this.data){
      this.data[cleanedKey].data = value.data
      this.data[cleanedKey].time = value.time
      this.data[cleanedKey].units = value.units
      this.data[cleanedKey].nodeID = id
    } else {
      this.data[cleanedKey] = value
      this.data[cleanedKey].nodeID = id
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
    return this.data["dissolved_o2"]["data"] && this.data["dissolved_o2"]["time"] && this.data["dew_point"]["data"] && this.data["dew_point"]["time"]
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
    nomad.subscribe(subscriptions, function(message) {
      console.log(message.message)
      messageData = JSON.parse(message.message)
      try{
        dataManager.setValue(messageData.columnNames[5], message.id, {data: messageData.data[5], time: messageData.data[0], units: messageData.columnUnits[5]})
      }
      catch(err){
        console.log("DataMaintainer failed with error of " + err)
      }
      console.log(dataManager.toString())
      let currentTime = getTime()
      let timeSince = currentTime - lastPub
      if (timeSince >= frequency){
        console.log("==========>timeSince >= timeBetween")
        console.log('dataManager.isAllFilled?:', dataManager.isAllFilled())
        if (dataManager.isAllFilled()){
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


