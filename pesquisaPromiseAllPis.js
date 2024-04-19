const fs = require('fs');
const path = require('path');
const { MongoClient } = require('mongodb');

// Configurações
const diretorioEntrada = 'D:/pesquisas_para_fazer'; // Atualize com o caminho da pasta de entrada
const diretorioSaida = 'D:/Pesquisas_feitas'; // Atualize com o caminho da pasta de saída
const colecao = 'PISJuntos';
const urlMongoDB = 'mongodb://localhost:27017'; // Atualize com a URL do seu MongoDB

if (!fs.existsSync(diretorioSaida)) {
  fs.mkdirSync(diretorioSaida, { recursive: true });
}

// Função para ler arquivo JSON
function lerArquivoJSON(caminhoArquivo) {
  try {
    const conteudo = fs.readFileSync(caminhoArquivo, 'utf8');
    const documentos = JSON.parse(conteudo);
    console.log('Documentos lidos do arquivo JSON:', documentos);
    return documentos;
  } catch (error) {
    console.error('Erro ao ler arquivo JSON:', error);
    return [];
  }
}

// Função para consultar MongoDB e salvar resultado em arquivo
async function consultarMongoDBESalvar(documentos, caminhoSaida, nomeArquivoEntrada) {
  try {
    const client = await MongoClient.connect(urlMongoDB, { useUnifiedTopology: true });
    const db = client.db('PISNovo');
    const resultados = [];

    for (const documento of documentos) {
      console.log(`Procurando PIS ${documento}`);
      const resultadoConsulta = await db.collection(colecao).find({ PIS: documento }).project({ _id: 0, PIS: 1, CPF: 1, NOME: 1 }).toArray();

      if (resultadoConsulta.length === 0) {
        console.log(`PIS ${documento} não encontrado.`);
        resultados.push({ PIS: documento, CPF: null, Nome: null }); // CPF e Nome são null se não encontrado
      } else {
        resultados.push({ PIS: documento, CPF: resultadoConsulta[0].CPF, NOME: resultadoConsulta[0].NOME });
      }
    }

    await client.close();
    console.log('Conexão fechada com o MongoDB.');

    // Gera arquivo JSON com os resultados na pasta de saída
    const caminhoArquivoResultado = path.join(caminhoSaida, `${nomeArquivoEntrada}_resultados.json`);
    fs.writeFileSync(caminhoArquivoResultado, JSON.stringify(resultados, null, 2));
    console.log(`Arquivo JSON com resultados gerado em ${caminhoArquivoResultado}.`);
  } catch (error) {
    console.error('Erro ao consultar MongoDB e salvar resultado:', error);
  }
}

// Função principal para processar arquivos JSON de forma incremental
async function processarArquivosIncrementalmente() {
  try {
    const arquivos = fs.readdirSync(diretorioEntrada);

    for (const arquivo of arquivos) {
      if (path.extname(arquivo) === '.json') {
        const caminhoArquivo = path.join(diretorioEntrada, arquivo);
        const documentos = lerArquivoJSON(caminhoArquivo);

        if (documentos.length > 0) {
          console.log(`Processando arquivo ${arquivo}...`);
          await consultarMongoDBESalvar(documentos, diretorioSaida, path.basename(arquivo, '.json'));
        } else {
          console.error(`Erro ao processar o arquivo JSON ${arquivo}: Nenhum documento encontrado.`);
        }
      }
    }

    console.log('Processamento incremental concluído.');
  } catch (error) {
    console.error('Erro ao processar arquivos JSON de forma incremental:', error);
  }
}

// Executa a função principal para processar os arquivos de forma incremental
processarArquivosIncrementalmente();
