document.addEventListener('DOMContentLoaded', function() {
  const loginForm = document.getElementById('loginForm');
  const emailInput = document.getElementById('email');
  const passwordInput = document.getElementById('password');
  const googleBtn = document.querySelector('.google-btn');
  const createAccountLink = document.querySelector('.create-account');
  const forgotPasswordLink = document.querySelector('.click-here');

  // Handle form submission
  loginForm.addEventListener('submit', function(e) {
    e.preventDefault();
    
    const email = emailInput.value.trim();
    const password = passwordInput.value.trim();
    
    if (!email || !password) {
      alert('Please fill in all fields');
      return;
    }
    
    if (!isValidEmail(email)) {
      alert('Please enter a valid email address');
      return;
    }
    
    // Simulate login process
    console.log('Login attempt:', { email, password: '***' });
    alert('Login functionality would be implemented here');
  });

  // Handle Google login
  googleBtn.addEventListener('click', function() {
    console.log('Google login clicked');
    alert('Google login functionality would be implemented here');
  });

  // Handle create account link
  createAccountLink.addEventListener('click', function(e) {
    e.preventDefault();
    console.log('Create account clicked');
    alert('Create account functionality would be implemented here');
  });

  // Handle forgot password link
  forgotPasswordLink.addEventListener('click', function(e) {
    e.preventDefault();
    console.log('Forgot password clicked');
    alert('Forgot password functionality would be implemented here');
  });

  // Email validation function
  function isValidEmail(email) {
    const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
    return emailRegex.test(email);
  }

  // Add input focus effects
  const inputs = document.querySelectorAll('input');
  inputs.forEach(input => {
    input.addEventListener('focus', function() {
      this.parentElement.classList.add('focused');
    });
    
    input.addEventListener('blur', function() {
      this.parentElement.classList.remove('focused');
    });
  });
});
