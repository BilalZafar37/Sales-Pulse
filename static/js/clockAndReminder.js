function playReminder() {
    const audioElement = document.getElementById('reminder-audio');
    let robote = document.getElementById("robote");

    // Set up the event listener to show the 'robote' element when the audio starts playing.
    audioElement.onplay = () => {
        robote.style.display = '';
    };

    // Set up the event listener to hide the 'robote' element when the audio ends.
    audioElement.onended = () => {
        robote.style.display = 'none';
    };

    // Fetch the audio and set the source
    fetch('/generate-audio?text=' + encodeURIComponent('Reminder to update all order statuses before the daily reporting cut-off in 30 minutes'))
        .then(response => response.blob())
        .then(blob => {
            const audioURL = URL.createObjectURL(blob);
            audioElement.src = audioURL;

            // Play the audio
            audioElement.play()
                .catch(error => {
                    console.error('Error playing audio:', error);
                    robote.style.display = 'none'; // Hide 'robote' if there's an error playing audio
                });
        })
        .catch(error => {
            console.error('Error generating audio:', error);
            robote.style.display = 'none'; // Hide 'robote' if there's an error generating the audio
        });
}


function CountdownTracker(label, value){

    var el = document.createElement('span');

    el.className = 'flip-clock__piece';
    el.innerHTML = '<b class="flip-clock__card flip-card"><b class="flip-card__top"></b><b class="flip-card__bottom"></b><b class="flip-card__back"><b class="flip-card__bottom"></b></b></b>' + 
    '<span class="flip-clock__slot">' + label + '</span>';

    this.el = el;

    var top = el.querySelector('.flip-card__top'),
        bottom = el.querySelector('.flip-card__bottom'),
        back = el.querySelector('.flip-card__back'),
        backBottom = el.querySelector('.flip-card__back .flip-card__bottom');

    this.update = function(val){
    val = ( '0' + val ).slice(-2);
    if ( val !== this.currentValue ) {

        if ( this.currentValue >= 0 ) {
        back.setAttribute('data-value', this.currentValue);
        bottom.setAttribute('data-value', this.currentValue);
        }
        this.currentValue = val;
        top.innerText = this.currentValue;
        backBottom.setAttribute('data-value', this.currentValue);

        this.el.classList.remove('flip');
        void this.el.offsetWidth;
        this.el.classList.add('flip');
    }
    }

    this.update(value);
}

// Calculation adapted from https://www.sitepoint.com/build-javascript-countdown-timer-no-dependencies/

function getTime() {
    var t = new Date();
    return {
    'Total': t,
    'Hours': t.getHours() % 12 || 12, // Convert to 12-hour format
    'Minutes': t.getMinutes(),
    'Seconds': t.getSeconds()
    };
}

function Clock() {
    var updateFn = getTime;

    this.el = document.createElement('div');
    this.el.className = 'flip-clock';

    var trackers = {},
        t = updateFn(),
        key, timeinterval;

    for ( key in t ){
    if ( key === 'Total' ) { continue; }
    trackers[key] = new CountdownTracker(key, t[key]);
    this.el.appendChild(trackers[key].el);
    }

    var i = 0;
    function updateClock() {
    timeinterval = requestAnimationFrame(updateClock);

    // Throttle so it's not constantly updating the time.
    if ( i++ % 10 ) { return; }

    var t = updateFn();
    for ( key in trackers ){
        trackers[key].update( t[key] );
    }
    }

    setTimeout(updateClock,500);
}

// Initialize the clock
var clock = new Clock();
document.body.appendChild(clock.el);


