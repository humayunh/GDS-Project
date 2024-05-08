function showSection(sectionId) {

    document.getElementById('home-section').style.display = 'none';
    document.getElementById('graphs-section').style.display = 'none';

    switch(sectionId) {
        case 'home':
            document.getElementById('home-section').style.display = 'block';
            break;
        case 'graphs':
            document.getElementById('graphs-section').style.display = 'block';
            break;

    }
}

// Initially set the Home section to display
showSection('home');
