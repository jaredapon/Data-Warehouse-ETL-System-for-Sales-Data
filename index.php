<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>ETL Pipeline</title>
    <link rel="stylesheet" href="css/styles.css">
    <script src="js/scripts.js"></script>
</head>
<body>
    <!-- Loading Screen -->
    <div id="loading-screen" class="loading-screen">
        <div class="loading-content">
            <div class="spinner"></div>
            <p>Processing files, please wait...</p>
        </div>
    </div>

    <!-- Form for uploading the CSV files -->
    <form action="upload.php" method="post" enctype="multipart/form-data" onsubmit="submitForm(event)">
        <h1>ETL Pipeline</h1>
        <label class="custom-file-upload" onclick="document.getElementById('file-input').click();">
            Choose CSV Files
        </label>
        <input type="file" id="file-input" name="csv_files[]" accept=".csv" multiple onchange="displaySelectedFiles(this)">
        <div id="file-list"></div>
        <button type="submit">Upload</button>
    </form>
</body>
</html>
