document.addEventListener('DOMContentLoaded', function() {
    // For tooltips
    $('[data-toggle="tooltip"]').tooltip();
    
    var dropdown = document.getElementsByClassName("dropbtn");
    var i;

    // On page load, check localStorage to set the initial state of dropdowns
    for (i = 0; i < dropdown.length; i++) {
        var dropdownId = dropdown[i].getAttribute('data-id'); // Add a unique identifier for each dropdown
        var storedState = localStorage.getItem('dropdown_' + dropdownId);

        // If localStorage indicates the dropdown should be open, apply the open state
        if (storedState === 'open') {
            dropdown[i].classList.add('active');
            dropdown[i].setAttribute("aria-expanded", true);

            var caretIcon = dropdown[i].querySelector('.fas');
            caretIcon.classList.add('fa-caret-down');
            caretIcon.classList.remove('fa-caret-right');

            var dropdownContent = dropdown[i].nextElementSibling;
            dropdownContent.style.display = "block";
        }
    }

    // Add event listeners to toggle dropdowns and update localStorage
    for (i = 0; i < dropdown.length; i++) {
        dropdown[i].addEventListener("click", function() {
            var dropdownId = this.getAttribute('data-id'); // Get the unique identifier for each dropdown
            this.classList.toggle("active");

            // Toggle the aria-expanded attribute for accessibility
            var isExpanded = this.getAttribute("aria-expanded") === "true";
            this.setAttribute("aria-expanded", !isExpanded);

            // Toggle caret icon direction
            var caretIcon = this.querySelector('.fas');
            // caretIcon.classList.toggle('fa-caret-right');
            // caretIcon.classList.toggle('fa-caret-down');

            // Toggle the dropdown content visibility
            var dropdownContent = this.nextElementSibling;
            if (dropdownContent.style.display === "block") {
                dropdownContent.style.display = "none";
                localStorage.setItem('dropdown_' + dropdownId, 'closed'); // Store the state as 'closed'
            } else {
                dropdownContent.style.display = "block";
                localStorage.setItem('dropdown_' + dropdownId, 'open'); // Store the state as 'open'
            }
        });
    }
});


// document.addEventListener('DOMContentLoaded', function() {
//     // Function to check for unseen messages
//     function checkUnseenMessages() {
//         fetch('/check_unseen_messages')
//             .then(response => response.json())
//             .then(data => {
//                 if (data.status === 'success' && data.NumUnseen === 'exists') {
//                     startFlashingIcon();
//                 } else {
//                     stopFlashingIcon();
//                 }
//             })
//             .catch(error => {
//                 console.error('Error fetching unseen messages:', error);
//             });
//     }

//     // Variables to manage the flashing interval
//     let flashInterval;
//     const iconElement = document.getElementById('chat-icon');
//     // const tabElement = document.getElementById('tab');

//     function startFlashingIcon() {
//         if (!flashInterval) {
//             flashInterval = setInterval(() => {
//                 iconElement.classList.toggle('flashing');
//             }, 500); // Change color every 500ms
//         }
//     }

//     function stopFlashingIcon() {
//         if (flashInterval) {
//             clearInterval(flashInterval);
//             flashInterval = null;
//             iconElement.classList.remove('flashing');
//         }
//     }

//     // Event listener to stop flashing when the icon is clicked
//     iconElement.addEventListener('click', function() {
//         stopFlashingIcon();
//         // Optionally, reset NumUnseen on the server
//     });

//     // Check for unseen messages every 5 seconds
//     setInterval(checkUnseenMessages, 5000); // Adjust the interval as needed

//     // Initial check when the page loads
//     checkUnseenMessages();
// });

// 




// find every <input type="number" max="…">
document.querySelectorAll('input[type="number"][max]').forEach(input => {
// clamp on every keystroke / paste
input.addEventListener('input', e => {
    const el = e.target;
    // parse min/max from attributes
    const max = el.hasAttribute('max') ? parseFloat(el.max) : null;
    const min = el.hasAttribute('min') ? parseFloat(el.min) : null;
    let val = parseFloat(el.value);
    if (isNaN(val)) return;      // ignore empty / non-numeric
    if (max !== null && val > max) el.value = max;
    else if (min !== null && val < min) el.value = min;
});
});




