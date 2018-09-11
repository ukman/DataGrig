angular.module('dg.utils', [])
.factory('DGUtils', function(){
	return {
		// Preprocess table or query data from backend for displaying it in UI
        preprocessData: function(data) {
            for(var i = 0; i < data.data.length; i++) {
                var row = data.data[i];
                for(var j = 0; j < data.metaData.length; j++) {
                    var field = data.metaData[j];
                    var value = row[field.name];
                    if(field.type == 'timestamp' || field.type == 'date') {
                        if(Number.isInteger(value)) {
                        	row[field.name] = moment(value).format('DD-MM-YY H:mm:ss');
                            // row[field.name] = new Date(value);
                        }
                    }
                    /*
                    if(Number.isInteger(value) || Number.isFloat(value)) {
	                    var sum = $scope.total[field.name];
	                    sum = sum ? sum : 0;
	                    sum += value;
	                    $scope.total[field.name] = sum;
                    }
                    */
                }
            }
        },
        
        beautifyColName : function(name) {
        	var words = name.split('_');
        	for(var i = 0; i < words.length; i++) {
        		var word = words[i];
        		if(word.length > 0) {
        			word = word.substring(0, 1).toUpperCase() + word.substring(1);
        		}
        		words[i] = word;
        	}
        	return words.join(' ');
        }
	
		
	};
});
