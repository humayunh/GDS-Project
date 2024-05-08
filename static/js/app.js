function fetchRecommendations() {
    const userId = document.getElementById('user-id').value;
    if (!userId) {
        alert("User ID is required");
        return;
    }
    const url = `http://127.0.0.1:5001/recommendations?user_id=${encodeURIComponent(userId)}`;
    fetch(url)
        .then(response => response.json())
        .then(data => {
            const recommendationsElement = document.getElementById('recommendations');
            if (data.error) {
                recommendationsElement.innerHTML = `<p>Error: ${data.error}</p>`;
            } else {
                const recommendationsList = data.map(item => `<li>${item.movie} (Score: ${item.score})</li>`).join('');
                recommendationsElement.innerHTML = `<ul>${recommendationsList}</ul>`;
            }
        })
        .catch(error => {
            console.error('Error fetching data:', error);
            document.getElementById('recommendations').innerHTML = `<p>Error fetching data: ${error}</p>`;
        });
}
