const fs = require('fs');
const readline = require('readline');

const inputFilePath = 'input.txt'; // Путь к исходному файлу
const outputFilePath = 'sorted.txt'; // Путь к файлу с отсортированными данными
const chunkSize = 500 * 1024 * 1024; // Размер чанка (500 МБ)

async function externalSort(inputFilePath, outputFilePath, chunkSize) {
  const chunks = []; // Массив для хранения путей к временным файлам

  // Чтение исходного файла построчно и разделение на чанки
  const readStream = fs.createReadStream(inputFilePath);
  const rl = readline.createInterface({
    input: readStream,
    crlfDelay: Infinity
  });

  let lines = [];
  let currentChunkSize = 0;
  let chunkIndex = 0;

  for await (const line of rl) {
    lines.push(line);
    currentChunkSize += line.length + 1; // Учитываем символ новой строки

    if (currentChunkSize >= chunkSize) {
      // Текущий чанк достиг максимального размера, сортируем и сохраняем во временный файл
      lines.sort();
      const chunkPath = `chunk_${chunkIndex}.txt`;
      chunks.push(chunkPath);
      fs.writeFileSync(chunkPath, lines.join('\n'));

      // Подготовка для следующего чанка
      lines = [];
      currentChunkSize = 0;
      chunkIndex++;
    }
  }

  // Сортировка и сохранение последнего чанка (если есть)
  if (lines.length > 0) {
    lines.sort();
    const chunkPath = `chunk_${chunkIndex}.txt`;
    chunks.push(chunkPath);
    fs.writeFileSync(chunkPath, lines.join('\n'));
  }

  // Объединение чанков в отсортированный файл
  const outputStream = fs.createWriteStream(outputFilePath);
  const streams = chunks.map(chunk => fs.createReadStream(chunk));

  await Promise.all(streams.map(stream => {
    return new Promise((resolve, reject) => {
      stream.on('error', reject);
      stream.on('end', resolve);
      stream.pipe(outputStream, { end: false });
    });
  }));

  // Закрытие потоков и удаление временных файлов
  outputStream.end();
  streams.forEach(stream => stream.destroy());
  chunks.forEach(chunk => fs.unlinkSync(chunk));
}

// Запуск внешней сортировки
externalSort(inputFilePath, outputFilePath, chunkSize)
  .then(() => {
    console.log('Сортировка завершена. Результат сохранен в файле:', outputFilePath);
  })
  .catch(error => {
    console.error('Произошла ошибка при сортировке:', error);
  });
