window.onpageshow = function(event) {
    if (event.persisted) {
        window.location.reload() 
    }
};

$(document).ready(function() {
    var socket = io();
    var sales_document = $('#sales_document').val();
    var room = sales_document;  // Modify this later TODO
    var user = $('#user').val();

    var isMuted = false;
    var muteIcon = $('#mute_icon');

    // Check localStorage for mute setting for the room
    if (localStorage.getItem('mute-' + room) === 'true') {
        isMuted = true;
        muteIcon.text('volume_off');
        $('#mute-button').addClass('btn-danger');
    } else {
        isMuted = false;
        muteIcon.text('volume_up');
        $('#mute-button').addClass('btn-success');
    }

    // Function to scroll to the bottom of the chat div
    function scrollToBottom() {
        var chatMessages = $('#chat-messages');
        chatMessages.scrollTop(chatMessages.prop("scrollHeight"));
    }

    // Function to update unseen counts for all rooms
    function updateUnseenCounts() {
        $.getJSON('/unseen', function(data) {
            // Iterate through each unseen count and update the corresponding badge
            for (let [salesDoc, count] of Object.entries(data)) {
                let badge = $(`#unseen-count-${salesDoc}`);

                if (count > 0) {
                    badge.text(`+${count}`).show();
                } else {
                    badge.hide();  // Hide the badge if there are no unseen messages
                }
            }
        });
    }

    // Request Notification permission
    if ("Notification" in window && Notification.permission !== "granted") {
        Notification.requestPermission();
    }

    // Join the room on connection
    socket.on('connect', function() {
        socket.emit('join', {'room': room});
    });

    // Send a message when Enter key is pressed
    $('#chat-input').keypress(function(e) {
        if (e.which == 13 && $(this).val().trim() !== "") {
            var msg = $(this).val();
            
            // Get the values of SalesDocument, Delivery, and Site
            var delivery = $('#delivery').val();
            var site = $('#site').val();
            var brand = $('#brand').val();

            // Send the message along with other data to the server
            socket.emit('text2', {
                'msg': msg,
                'room': room,
                'sales_document': sales_document,
                'delivery': delivery,
                'site': site,
                'brand': brand,
                'user': user
            });
            
            $(this).val('');  // Clear the input field after sending
        }
    });

    // Mute/Unmute button handler
    $('#mute-button').on('click', function() {
        isMuted = !isMuted;
        localStorage.setItem('mute-' + room, isMuted);

        if (isMuted) {
            muteIcon.text('volume_off');
            $('#mute-button').removeClass('btn-success').addClass('btn-danger');
        } else {
            muteIcon.text('volume_up');
            $('#mute-button').removeClass('btn-danger').addClass('btn-success');
        }
    });


    // Listen for incoming messages and append them to the chat window
    socket.on('message', function(data) {
        console.log(data)
        var messageTimestamp = new Date(data.timestamp).toLocaleString([], {
            day: '2-digit',
            month: '2-digit',
            year: 'numeric',
            hour: '2-digit',
            minute: '2-digit'
        });
        if (data.is_own_message) {
            $('#chat-messages').append(
                '<div class="message_user">' +
                '<p><small><strong>' + data.username + '</strong> - ' + messageTimestamp + '</small></p>' +
                '<div class="bubble">' + data.msg + '</div>' +
                '</div>'
            );
            scrollToBottom();
        } else {
            $('#chat-messages').append(
                '<div class="message_other">' +
                '<p><small><strong>' + data.username + '</strong> - ' + messageTimestamp + '</small></p>' +
                '<div class="bubble_other">' + data.msg+ '</div>' +
                '</div>'
            );
            scrollToBottom();

            if (!isMuted) {
                console.log("ding should play")
                // Play notification sound
                // Create Audio Context
                var audioCtx = new (window.AudioContext || window.AudioContext)();

                // Function to unlock audio context
                
                if (audioCtx.state === 'suspended') {
                    audioCtx.resume();
                }
                   

                // Event listener for user interaction
                // document.getElementById('enable-sound').addEventListener('click', unlockAudio);

                // Function to play sound
                function playNotificationSound() {
                    // Load the audio file
                    fetch('/static/audio/ding.mp3')
                        .then(response => response.arrayBuffer())
                        .then(arrayBuffer => audioCtx.decodeAudioData(arrayBuffer))
                        .then(audioBuffer => {
                            var source = audioCtx.createBufferSource();
                            source.buffer = audioBuffer;
                            source.connect(audioCtx.destination);
                            source.start(0);
                        })
                        .catch(e => console.error('Error playing sound:', e));
                }

                playNotificationSound()

                // Show browser notification
                if (Notification.permission === 'granted') {
                    var notification = new Notification('New message from ' + data.username, {
                        body: data.msg,
                        // icon: 'path/to/icon.png' // Optional
                    });

                    // Optional: focus the window when the notification is clicked
                    notification.onclick = function() {
                        window.focus();
                    };
                }
            }
        }
    });

    // Listen for the `update_unseen` event and update unseen counts for all rooms
    socket.on('update_unseen', function() {
        updateUnseenCounts();
    });

    // Load initial chat messages when the user joins the chat
    socket.on('initial_messages', function(messages) {
        messages.forEach(function(data, index) {
            var messageTimestamp = new Date(data.timestamp).toLocaleString([], {
                day: '2-digit',
                month: '2-digit',
                year: 'numeric',
                hour: '2-digit',
                minute: '2-digit'
            });

            // If it's the first message, give it the "status-message" class
            if (index === 0) {
                $('#chat-messages').append(
                    '<div class="status-message">' +
                    '<p><small>' + messageTimestamp + '</small></p>' +
                    '<p><em>' + data.message + '</em></p>' +
                    '</div>'
                );
            } else {
                // For subsequent messages, check if it's the user's message or others'
                if (data.is_own_message) {
                    $('#chat-messages').append(
                        '<div class="message_user">' +
                        '<p><small><strong>' + data.username + '</strong> - ' + messageTimestamp + '</small></p>' +
                        '<div class="bubble">' + data.message + '</div>' +
                        '</div>'
                    );
                } else {
                    $('#chat-messages').append(
                        '<div class="message_other">' +
                        '<p><small><strong>' + data.username + '</strong> - ' + messageTimestamp + '</small></p>' +
                        '<div class="bubble_other">' + data.message + '</div>' +
                        '</div>'
                    );
                }
            }
        });
        scrollToBottom();
    });

    // Initialize unseen counts on page load
    updateUnseenCounts();

    // Handle user mentions
    socket.on('mention', function(data) {
        alert(data.msg);
    });

    // Display the status of the user joining the room
    socket.on('status', function(data) {
        $('#chat-messages').append('<div class="status-message">' +data.msg + '</div>');
        scrollToBottom();
    });

    // Not supported in safari and opra
    window.onbeforeunload = function (e) {
        // e.returnValue = " ";
        var socket = io();
        socket.emit('leave', { 'room': room });
    };

    // Listen for clicks on all <a> tags in the document
    $('a').on('click', function(event) {
        
        console.log("User clicked on a link, emitting leave event.");
        var socket = io();
        // Emit leave event
        socket.emit('leave', { 'room': room });
    });
    
});

