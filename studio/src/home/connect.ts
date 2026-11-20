// Connection strings for each protocol this gateway listens on, from
// WhoAmI's listener ports and the host Studio was reached at. Passwords are
// never filled in; TLS options are left to the client's own flags.

export interface ConnectLine {
  protocol: string;
  /** What the line is for. */
  label: string;
  text: string;
}

const ORDER = ['mysql', 'mariadb', 'mongo', 'redis', 'bolt', 'kafka', 'amqp', 'amqps', 'https', 'http'];

/** One line per listener, in a fixed order; unknown listeners are left out. */
export function connectionLines(
  host: string,
  listeners: Readonly<Record<string, number>>,
  login: string,
  db: string | null,
): ConnectLine[] {
  const h = host.includes(':') && !host.startsWith('[') ? `[${host}]` : host;
  const path = db ? encodeURIComponent(db) : '';
  const out: ConnectLine[] = [];
  for (const p of ORDER) {
    const port = listeners[p];
    if (!port) continue;
    switch (p) {
      case 'mysql':
        out.push({ protocol: p, label: 'MySQL', text: `mysql -h ${host} -P ${port} -u ${login} -p${db ? ` ${db}` : ''}` });
        break;
      case 'mariadb':
        out.push({ protocol: p, label: 'MariaDB', text: `mariadb -h ${host} -P ${port} -u ${login} -p${db ? ` ${db}` : ''}` });
        break;
      case 'mongo':
        out.push({ protocol: p, label: 'MongoDB', text: `mongodb://${login}@${h}:${port}/${path}?authSource=admin` });
        break;
      case 'redis':
        out.push({ protocol: p, label: 'Redis', text: `redis-cli -h ${host} -p ${port} --user ${login} --askpass` });
        break;
      case 'bolt':
        out.push({ protocol: p, label: 'Bolt (Cypher)', text: `neo4j://${h}:${port}  (user ${login})` });
        break;
      case 'kafka':
        out.push({ protocol: p, label: 'Kafka', text: `bootstrap.servers=${h}:${port}` });
        break;
      case 'amqp':
      case 'amqps':
        out.push({ protocol: p, label: p === 'amqps' ? 'AMQP over TLS' : 'AMQP', text: `${p}://${login}@${h}:${port}/` });
        break;
      case 'https':
      case 'http':
        out.push({ protocol: p, label: p === 'https' ? 'REST (HTTPS)' : 'REST (HTTP)', text: `${p}://${h}:${port}/v1/` });
        break;
    }
  }
  return out;
}
