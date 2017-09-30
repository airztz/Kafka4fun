var stompClient = null;
var chartData = null;
var markets = {"NYC Manhattan": null, "NYC East": null, "Las Vegas": null}
var subscribe_callback = function(currentTput){
		var data = currentTput.body.split(",");		
		var market = data[0];
		var cTput = parseFloat(data[1]);
		var time = (new Date()).getTime();
		if(markets[market]!=null){	
			if(cTput<3000){	
			    var color_value = '#bf280b';
				markets[market].addPoint({x:time,y:cTput,color:color_value}, true, true);
		    }
			else {
				markets[market].addPoint([time, cTput], true, true);	
			}
		}
}
var connect_callback = function(frame){
	console.log('Connected: ' + frame);
	stompClient.subscribe('/topic/currentTput', subscribe_callback);
}
    var socket = new SockJS('/websocket');
    stompClient = Stomp.over(socket);
    stompClient.connect({}, connect_callback);
        
        Highcharts.setOptions({
        global: {
            useUTC: false
        }
    	});
    
        Highcharts.chart('Throughput_container', {
        chart: {
            type: 'spline',
            animation: Highcharts.svg, // don't animate in old IE
            marginRight: 10,
            events: {
                load: function () {
                    markets["NYC East"] = this.series[0]; 
                    markets["NYC Manhattan"] = this.series[1]; 
                    markets["Las Vegas"] = this.series[2];                                      
//                    setInterval(function () {  
//                    	var x = (new Date()).getTime();                  
//                        series.addPoint([x, Tput], true, true);
//                        Tput = 0;
//                    }, 1000);
                }
            }
        },
        title: {
            text: 'Current Video Throughput(Mbps)'
        },
        xAxis: {
            type: 'datetime',
            tickPixelInterval: null
        },
        yAxis: {
            title: {
                text: 'Tput(mbps)'
            },
            plotLines: [{
                value: 0,
                width: 1,
                color: '#808080'
            }]
        },
        tooltip: {
            formatter: function () {
                return '<b>' + this.series.name + '</b><br/>' +
                    Highcharts.dateFormat('%Y-%m-%d %H:%M:%S', this.x) + '<br/>' +
                    Highcharts.numberFormat(this.y, 2);
            }
        },
        legend: {
            enabled: false
        },
        exporting: {
            enabled: false
        },
        series: [
        {
            name: 'NYC East Video Throughput(Mbps)',
            color: '#efeb10',   //yellow
            data: (function () {
                // generate an array of random data for chart initialization
                var data = [],
                    time = (new Date()).getTime(),
                    i;

                for (i = -19; i <= 0; i += 1) {
                    data.push({
                        x: time + i * 1000,
                        y: Math.random()
                    });
                }
                return data;
            }())
        },
        {
            name: 'NYC Manhattan Video Throughput(Mbps)',
            color: '#0b58bf',   //blue
            data: (function () {
                // generate an array of random data for chart initialization
                var data = [],
                    time = (new Date()).getTime(),
                    i;
                for (i = -19; i <= 0; i += 1) {
                    data.push({
                        x: time + i * 1000,
                        y: Math.random()
                    });
                }
                return data;
            }())
        },
        {
            name: 'Las Vegas Video Throughput(Mbps)',
            color: '#000000',     //black
            data: (function () {
                // generate an array of random data for chart initialization
                var data = [],
                    time = (new Date()).getTime(),
                    i;
                for (i = -19; i <= 0; i += 1) {
                    data.push({
                        x: time + i * 1000,
                        y: Math.random()
                    });
                }
                return data;
            }())
        }
        ]
    });