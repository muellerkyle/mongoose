var object = require('./util').object,
    writerOptions = ['sort', 'limit', 'skip', 'snapshot', 'group'],
    promised = ['all', 'get', 'last', 'first', 'one'],
    ObjectID = require('mongodb').BSONPure.ObjectID,
    Class = require('./util').Class;
	
var aggragates = {
		$sum: function(g,vals){
			var res = {};
			if (vals.length > 0) // Build results obj
				for (var prop in vals[0])
					res[prop] = 0;
					
			for (var i=0;i<vals.length;i++) {
				for (var prop in vals[i])
					res[prop]+=(isNaN(vals[i][prop]) ? 0 : vals[i][prop]);
			}
			
			return res;
		},
		$count: function(g,vals) {
			var res = {val:0};
			if (Object.prototype.toString.call(vals) == '[object Array]')
				vals.forEach(function(item) { count += 1})
			return res;
		},
		$avg: function(g,vals) {
			var sum=0; len = 0;
			for (var i = 0; i < vals.length; i++) {
				sum += (isNaN(vals[i].val) ? 0 : vals[i].val); len++;
			}
			return { val: (sum / len)};
		}
	}

// todo add sugar

Writer = this.Writer = Class({
  init: function(onExec){
    this._query = {};
    this._options = {};
	this._aggRdc = false;
	this._aggMap = false;
    this._onExec = onExec;
	this.__defineGetter__("query", this.getQuery);
	this.__defineGetter__("options", this.getOptions);
	this.__defineGetter__("isCMD", this.determineCMD);
	this.__defineGetter__("map", this.getMapFN);
	this.__defineGetter__("reduce", this.getReduceFN);
  },
  
  // Getters
  getQuery: function() {
  	return this._query;
  },
  getOptions: function() {
  	return this._options;
  },
  determineCMD: function() {
	return typeof(this._aggRdc) == 'function';
  },
  getReduceFN: function() {
  	if (typeof(this._aggRdc) == 'function')
		return this._aggRdc;
  },
  getMapFN: function() {
  	if (typeof(this._aggMap) == 'function')
		return this._aggMap;
  },
  
  select: function() {
    if (arguments.length == 1 && typeof arguments[0] == 'object'){
		for (var i in arguments[0]) {
			this.select(i, arguments[0][i]);
		}
    } else if (arguments.length == 2) {
		if (/^\$/.test(arguments[0])){
			this._aggRdc = aggragates[arguments[0]]; // set reduce function
			if (typeof(this._aggRdc) == 'function') { // Set map function
				if (arguments[1] && (arguments[1].field || arguments[1].fields)){ // Only support of top level props for now
					var grpMeta = ((typeof arguments[1].group == 'string') ? 'this["'+arguments[1].group+'"]' : '"all"');
					var fieldSummary = '';
					if (arguments[1].field && typeof(arguments[1].field) == 'string')
						fieldSummary+= (fieldSummary.length>0?',':'') + arguments[1].field+':this["'+arguments[1].field+'"]';
					if (arguments[1].fields && arguments[1].fields.length != undefined)
						arguments[1].fields.forEach(function(field){
							if (typeof(field) == 'string')
								fieldSummary+=(fieldSummary.length>0?',':'')+field +':this["'+field+'"]';
						})
					this._aggMap = new Function('emit('+grpMeta+', {'+fieldSummary+'})');
				}	
			}
		}
		else
    		this._options[arguments[0]] = arguments[1];
    }
  	return this;
  },
  
  where: function(){
    if (arguments.length == 1 && typeof arguments[0] == 'object'){
      for (var i in arguments[0]) this.where(i, arguments[0][i]);
    } else if (arguments.length == 2) {
      // temporary, we should probably cast all?
      if (/^_/.test(arguments[0])) { // Only do a conversion if the value is not an object. (This keeps $in from breaking)
		if (typeof arguments[1] != 'object')
			arguments[1] = arguments[1].toHexString ? arguments[1] : ObjectID.createFromHexString(arguments[1]);
	  }
      this._query[arguments[0]] = arguments[1];
    }
    return this;
  },
  
  exec: function(){
    if (!this.__promise){
      this.__promise = new Promise();
      if (this._onExec) this._onExec.apply(this, [this.__promise])
    }
    return this.__promise;
  }
  
}),

Promise = this.Promise = Class({
  
  init: function(){
    this._queues = [[]];
  },
  
  stash: function(){
    this._queues.push([]);
    return this;
  },
  
  _queue: function(method, args){
    for (var i = this._queues.length - 1; i >= 0; i--){
      if (this._queues[i] !== null){
        this._queues[i].push([method, args]);
        return this;
      }
    }
    this['_' + method](args[0], this.data);
    return this;
  },
  
  complete: function(data){
    for (var i = 0, l = this._queues.length; i < l; i++){
      if (this._queues[i] !== null){
        this._queues[i].forEach(function(call){
          this['_' + call[0]](call[1][0], data);
        }, this);
        this._queues[i] = null;
        this.data = data;
        return this;
      }
    }
    return this;
  },
  
  _one: function(callback, data){
    if(callback) callback.apply(this, [data[0] || null]);
  },
  
  _first: function(callback, data){
    if(callback) callback.apply(this, [data[0] || null]);
  },
  
  _last: function(callback, data){
    if(callback) callback.apply(this, [data[data.length - 1] || null]);
  },
  
  _get: function(callback, data){
    if(callback) callback.apply(this, [data]);
  },
  
  _all: function(callback, data){
    if(callback) callback.apply(this, [data]);
  }
  
});

// add option altering ones automatically
writerOptions.forEach(function(option){
  Writer.prototype[option] = function(o){
    this._options[option] = o;
    return this;
  };
});

promised.forEach(function(method){
  // we make sure the promised methods trigger execution automatically of the writer
  Writer.prototype[method] = function(){
    var promise = this.exec();
    promise[method].apply(promise, arguments);
    return promise;
  };
  
  // we wrap the methods in the promise
  Promise.prototype[method] = function(){
    return this._queue(method, arguments);
  };
});