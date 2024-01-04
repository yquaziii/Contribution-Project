// Sample contribution data (replace this with your actual data)
const contributionData = [0, 2, 5, 7, 3, 0, 1];

document.addEventListener('DOMContentLoaded', () => {
    const contributionDays = document.querySelectorAll('.contribution-day');
    
    contributionDays.forEach((day, index) => {
        day.style.height = `${contributionData[index] * 30}px`;
    });
});
