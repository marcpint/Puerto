#include <sys/socket.h>
#include <sys/types.h>
#include <stdlib.h>
#include <stdio.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <libpq-fe.h>
#include <time.h>
#include <string.h>
#include <unistd.h>
#include <ctype.h>

#define SUCCESS 0
#define ERROR   1
#define END_LINE 0x0A
#define MAX_MSG 2000
//#define MAX_MSG 700

typedef struct {
	int ident;
	char av_serie[20];
	time_t tiempo;
	int tipo_c;
	char cabecera;
	char resto[200];
} s_casilla ;

typedef struct _nodoM {
	char *seq;
	char *av_serie;
	char *mensaje;
	time_t tiempo;
	struct _nodoM *sig;
	struct _nodoM *ant;
} s_mensaje ;

typedef struct Ecola {
	s_mensaje *inicio;
	s_mensaje *fin;
	time_t lapso;
	int max_inid;
} lista ;

int Abre_Socket_Inet (int vSERVER_PORT);
void nuevoCliente (int MAX_CLIENTES, int servidor, s_casilla *clientes, int *nClientes);
int dameMaximo (s_casilla *tabla, int n);
void compactaClaves (s_casilla *tabla, int *n);
int Escribe_Socket (int fd, char *Datos, int Longitud);
int Acepta_Conexion_Cliente (int Descriptor);
int read_line(s_casilla *v_cas, int v_puerto, char *servidor, int v_dia, int *v_s, char* tip[]);
int nuevoMsj(lista *vlis, char *msj, char *id, char *vseq);
void llenarLista(lista *vlis, char *serv, int vport);
void MsjEnviado(lista *vlis, s_mensaje *leec, int k, char *serv);

int main (int argc, char *argv[]) 
{
	int MAX_CLIENTES = atoi(argv[2]);
	int vSERVER_PORT = atoi(argv[1]);
	char *V_SERVIDOR = argv[3];
	
	//int vpuerto = atoi(argv[3]);
	int socketServidor;				/* Descriptor del socket servidor */
	s_casilla socketCliente[MAX_CLIENTES];/* Descriptores de sockets con clientes */
	int numeroClientes = 0;			/* Número clientes conectados */
	fd_set descriptoresLectura;	/* Descriptores de interes para select() */
	int buffer;							/* Buffer para leer de los socket */
	int maximo;							/* Número de descriptor más grande */
	int i, k;								/* Para bubles */
	time_t tf, tiempo;
	struct tm *tlocal;
    char output[2];
	int dia;
	int salida = 0;
	int v_lon;
	static char *tip[5] = {"SLU","RESP","REV","BUFF","RTX"};
	char ts[100];
	
	socketServidor = Abre_Socket_Inet (vSERVER_PORT);
	if (socketServidor == -1)
	{
		perror ("Error al abrir servidor");
		exit (-1);
	}
	
	lista *vl;
	if ((vl = (lista *) calloc (1, sizeof (lista))) == NULL)
		return -1;	
	vl->inicio = NULL;
	vl->fin = NULL;
	vl->max_inid = 0;
	s_mensaje *leeM;
	
	printf("Escuchando por el puerto TCP %u \n",vSERVER_PORT);
	while (salida ==0){
		(void) time(&tf);
		compactaClaves (socketCliente, &numeroClientes);
		FD_ZERO (&descriptoresLectura);
		FD_SET (socketServidor, &descriptoresLectura);
		for (i=0; i<numeroClientes; i++)
				FD_SET (socketCliente[i].ident, &descriptoresLectura);
		maximo = dameMaximo (socketCliente, numeroClientes);
		if (maximo < socketServidor)
			maximo = socketServidor;
		tiempo = time(0);
		tlocal = localtime(&tiempo);
		strftime(ts,100,"%c", tlocal);
		printf("%s :: Cantidad de conexiones %d\n", ts, numeroClientes);
		select (maximo + 1, &descriptoresLectura, NULL, NULL, NULL);
		strftime(output,2,"%w",tlocal);
		dia = atoi(output);
		if (dia == 0){
			dia = 7;
		}
		if (FD_ISSET (socketServidor, &descriptoresLectura)) {
			nuevoCliente (MAX_CLIENTES, socketServidor, socketCliente, &numeroClientes);
		} else {
			//aqui para escribirle al dispositivo
			if (vl->inicio == NULL || ((int) tf - vl->lapso) > 70) {
				llenarLista(vl, V_SERVIDOR, vSERVER_PORT);
				(void) time(&vl->lapso) ;
			}
			leeM = vl->inicio;
			for (i=0; i<numeroClientes && salida ==0; i++) //recorre conectados
			{ 
				if (FD_ISSET (socketCliente[i].ident, &descriptoresLectura)) {
					buffer = read_line(&socketCliente[i], vSERVER_PORT, V_SERVIDOR, dia, &salida, tip);
					if (buffer > 0) {
						if(strlen(socketCliente[i].av_serie) > 0) {
							(void) time(&socketCliente[i].tiempo) ;
							for (k=0; k<numeroClientes; k++) {
								if (k != i && strcmp( socketCliente[k].av_serie, socketCliente[i].av_serie ) == 0){
									close(socketCliente[k].ident);
									socketCliente[k].ident = -1;
								}
							}
						}
						//if (vl->inicio != NULL) {
						while (leeM != NULL) {
							if ( strcmp(leeM->av_serie, socketCliente[i].av_serie ) == 0){
								v_lon = strlen(leeM->mensaje);
								k = Escribe_Socket (socketCliente[i].ident, leeM->mensaje, v_lon);
								printf("respuesta %d\n", k);
								MsjEnviado(vl, leeM, k, V_SERVIDOR);
								leeM = NULL;
							} else {
								leeM = leeM->sig;
							}
						}
					}
				}
				else {
					//(void) time(&tf);
					if (strlen(socketCliente[i].av_serie) == 0 && ((int) tf - socketCliente[i].tiempo) > 70) {
						close(socketCliente[i].ident);
						socketCliente[i].ident = -1;
					}
					 if (strlen(socketCliente[i].av_serie) > 0 && ((int) tf - socketCliente[i].tiempo) > 7200) {
						close(socketCliente[i].ident);
						socketCliente[i].ident = -1;
					}
				}
			}
			if (salida == 1) {
				for (i=0; i<numeroClientes; i++) {
					close(socketCliente[i].ident);
					socketCliente[i].ident = -1;
				}
				close(socketServidor);
				socketServidor = -1;
				//printf("saliendo fin \n");
			}
		}
		
	}
	free(vl);
	return 0;
}

int Abre_Socket_Inet (int vSERVER_PORT)
{
	struct sockaddr_in dire;
	//socklen_t Longitud_Cliente;
	//struct servent *Puerto;
	int Descriptor;

	Descriptor = socket (AF_INET, SOCK_STREAM, 0);
	if (Descriptor == -1)
	 	return -1;
	dire.sin_family = AF_INET;
	dire.sin_port = htons(vSERVER_PORT);
	dire.sin_addr.s_addr = htonl(INADDR_ANY);
	int yes = 1;
	if (setsockopt(Descriptor,SOL_SOCKET,SO_REUSEADDR,&yes,sizeof(int)) == -1) {
		perror("setsockopt");
		exit(1);
	}
	if (bind (Descriptor,(struct sockaddr *) &dire, sizeof(dire)) == -1)
	{
		close (Descriptor);
		perror("cannot bind port");
		return -1;
	}
	if (listen (Descriptor, 7) == -1)
	{
		close (Descriptor);
		return -1;
	}
	return Descriptor;
}

void compactaClaves (s_casilla *tabla, int *n)
{
	int i,j;
	if ((tabla == NULL) || ((*n) == 0)){
		return;
	}
	j=0;
	for (i=0; i<(*n); i++)
	{
		if ( tabla[i].ident != -1 ) {
			if (i != j) tabla[j] = tabla[i];
			j++;
		}
	}	
	*n = j;
}

int dameMaximo (s_casilla *tabla, int n)
{
	int i;
	int max;
	if ((tabla == NULL) || (n<1))
		return 0;
	max = tabla[0].ident;
	for (i=0; i<n; i++)
		if (tabla[i].ident > max)
			max = tabla[i].ident;
	return max;
}

void nuevoCliente (int MAX_CLIENTES, int servidor, s_casilla *clientes, int *nClientes)
{
	clientes[*nClientes].ident = Acepta_Conexion_Cliente (servidor);
	(*nClientes)++;
	if ((*nClientes) >= MAX_CLIENTES)
	{
		close (clientes[(*nClientes) -1].ident);
		(*nClientes)--;
		return;
	}
	else
	{
		memset(clientes[*nClientes-1].av_serie, 0x0, 20);
		memset(clientes[*nClientes-1].resto, 0x0, 200);
		(void) time(&clientes[*nClientes-1].tiempo) ;
		clientes[*nClientes-1].tipo_c = -1;
	}
	/* char *ms ;
	ms = "";
	int v_lon = strlen(ms);
	if (v_lon > 0){
		int r_escri = Escribe_Socket (clientes[(*nClientes)-1].ident, ms, v_lon);
	} 
	return;*/
}

int Acepta_Conexion_Cliente (int Descriptor)
{
	socklen_t Longitud_Cliente;
	struct sockaddr_in Cliente;
	int Hijo; //aqui
	Longitud_Cliente = sizeof (Cliente);
	Hijo = accept (Descriptor, (struct sockaddr *) &Cliente, &Longitud_Cliente);
	if (Hijo == -1)
		return -1;
	//log registrar ip origen
	//printf("OK conext Cliente %s por TCP %d \n",inet_ntoa(Cliente.sin_addr),ntohs(Cliente.sin_port));
	return Hijo;
}

int Escribe_Socket (int fd, char *Datos, int Longitud) 
{
	int Escrito = 0;
	int Aux = 0;
	if ((fd == -1) || (Datos == NULL) || (Longitud < 1))
		return -1;
	while (Escrito < Longitud) {
		//printf ("dato : %s\n", Datos);
		Aux = write (fd, Datos, Longitud);
		if (Aux > 0) {
			Escrito = Escrito + Aux;
		} else {
			if (Aux == 0)
				return Escrito;
			else
				return -1;
		}
	}
	return Escrito;
}

int read_line(s_casilla *v_cas, int v_puerto, char *servidor, int v_dia, int *v_s, char* tipos[]) {
	
	int newSd = (*v_cas).ident;
	static int rcv_ptr=0;
	static char rcv_msg[MAX_MSG];
	static int n; //,i;
	int offset;
	/*static char *tipos[4];
	tipos[0] = "SLU";
	tipos[1] = "RESP";
	tipos[2] = "REV";
	tipos[3] = "BUFF"; */
	//int f_tipo = -1;

  	PGconn *conexion;
	PGresult *R;
	char *usuario = "puerto";
	char *clave = "Puerto";
	char *db = "bermann_gps_puerto";
	char exit[] = "CERRAR";
	offset=0;
	char real_msg[MAX_MSG];
	memset(real_msg,0x0,MAX_MSG);
	int flg1 = 0; 
	char *b_msg;
	int v_tipo = 0;
	char v_id;
	int j = 0;
	int flg_id = 1;
	int flg_id2 = 0;
	int k = 0, z, v_est = 1;
	char *datoId;
	char v_aserie[20];
	//char head ;
	char *v_ini;
	char consu[MAX_MSG+9000];
	while(1) { 
		if(rcv_ptr==0) {
			memset(rcv_msg,0x0,MAX_MSG); /* init buffer */
			n = recv(newSd, rcv_msg, MAX_MSG-1, MSG_DONTWAIT); /* wait for data probar con buffer mas pequeño y saber que sucede */
			//printf("%d INFO %s",n, rcv_msg);
			if (flg1 == 0){
				if (n<0) {
					perror(" cannot receive data ");
					return n;
				} else if (n==0) { 
					printf(" connection closed by client : %s \n", (*v_cas).av_serie);
					close(newSd);
					(*v_cas).ident = -1;
					return n;
				} else {
					if (strcmp(exit, rcv_msg) == 0) 
						(*v_s)++;
				}
			
			b_msg = (char *) calloc (MAX_MSG, sizeof(char));
			}
		} 
		strcpy(b_msg, rcv_msg);
		while (v_tipo < 5 && ((*v_cas).tipo_c  == -1 || (*v_cas).tipo_c  == 2 || (*v_cas).tipo_c  == 4)) {
			if (strstr(b_msg, tipos[v_tipo]) && strstr(b_msg, tipos[v_tipo]) - b_msg == 1)	{
				//head = rcv_msg[0];
				flg_id2++;
				(*v_cas).cabecera = rcv_msg[0];
				(*v_cas).tipo_c = v_tipo;
				switch (v_tipo) {
					case 0 :
						datoId = strtok(b_msg + 4, ",");
						if(strlen(datoId) == 6) {
							sprintf(v_aserie, "%d", (int) strtol(datoId,NULL, 16));
							datoId = v_aserie;
						}
					break;
					case 1 :
					case 3 :
						datoId = strtok(b_msg, ",");
						for (z= 0; z <2; z++)
							datoId = strtok(NULL, ",");
					break;
					case 2 :
						datoId = strtok(b_msg, ";");
						while(strstr(datoId, "ID=") == NULL)
							datoId = strtok(NULL, ";");
						datoId = strtok(datoId + 3, "<");
						//ENVIO DE ACK
						printf("ACK %s \n", datoId);
						write (newSd, datoId, 15); //Se envia el IMEI COMO ACK AL GPS
					break;
					case 4 :
						datoId = strtok(b_msg, ";");
						while(strstr(datoId, "ID=") == NULL)
							datoId = strtok(NULL, ";");
						datoId = strtok(datoId + 3, "<");
						v_est = 5;
					break;
				}
				for (z = 0; z < strlen(datoId) && flg_id ; z++){
					flg_id = isdigit(*(datoId + z));
				}
				if (!flg_id) {
					(*v_cas).tipo_c = -1;
					v_tipo = 5; //para salir del ciclo
				} else {
					//if (strlen((*v_cas).av_serie) == 0)
					sprintf((*v_cas).av_serie, "%s", datoId);
				}
			}
			v_tipo++;
		}
		
		free(b_msg);
		if (strlen((*v_cas).resto) > 0) {
			printf("hay resto \n");
			sprintf(real_msg, "%s", (*v_cas).resto);
			memset((*v_cas).resto, 0x0, 200);
			flg1++;
		}
		if (flg1 > 0) {
			z = strlen(real_msg);
			if (n == -1) {
				b_msg = (char *) calloc (MAX_MSG, sizeof(char));
				//memset(b_msg, 0x0, z);
			} else {
				b_msg = (char *) calloc ((MAX_MSG+z), sizeof(char));
				//memset(b_msg, 0x0, (n+z));
			}
			strcpy(b_msg, real_msg);
			strcat(b_msg, rcv_msg);
		} else {
			//printf("normal \n");
			b_msg = (char *) calloc (MAX_MSG, sizeof(char));
			//memset(b_msg, 0x0, n);
			strcpy(b_msg, rcv_msg);
		}
		
		/*if (flg1 > 0) strcat(b_msg, real_msg);
		strcat(b_msg, rcv_msg);*/
		memset(consu, 0x0, MAX_MSG+9000);
		datoId = b_msg;
		if ((*v_cas).tipo_c > -1) {
			sprintf(consu, "insert into mensaje_ms_%d (ms_contenido, ms_puerto, av_serie, ms_estado_inv) values ", v_dia);
			do {
				v_ini = datoId;
				datoId  = strchr(v_ini + 1, (*v_cas).cabecera);
				if ( datoId ) {
					j = datoId - v_ini;
					v_id = ',';
				} else {
					j = strlen(v_ini);
					v_id = ';';
					k = strlen(consu);
				}
				if (toascii(*(v_ini + j - 1)) == 10) {
					j = j - 2;
				} else if (toascii(*(v_ini + j - 1)) == 13) {
					j = j - 1;
				}
				memset(real_msg, 0x0, MAX_MSG);
				//j = strxfrm(real_msg, v_ini, j );
				strncpy(real_msg, v_ini, j );
				if (!datoId && (n == MAX_MSG || ((*v_cas).tipo_c == 0 && toascii(rcv_msg[n-1]) != 10 && toascii(rcv_msg[n-1]) != 13 ))) {
					if (consu[(k-2)] == ',') consu[(k-2)] = ';';
					sprintf((*v_cas).resto, "%s", real_msg);
					break;
				} else {
					//sprintf(consu,"%s( '%s', %d, %s, 1)%c ",consu,real_msg, v_puerto, (*v_cas).av_serie, v_id);
					sprintf(consu,"%s( '%s', %d, %s, %d)%c ",consu,real_msg, v_puerto, (*v_cas).av_serie, v_est, v_id);
				}
			} while (datoId );
		} else {
			sprintf(consu, "insert into mensaje_ms_%d (ms_contenido, ms_puerto, ms_estado) values ('%s', %d, 3) RETURNING ms_id;",v_dia , b_msg, v_puerto);
		}
		//free(b_msg);
		//printf("consulta: %s\n", consu);
		conexion = PQsetdbLogin(servidor,"5434",NULL,NULL,db,usuario,clave);
		if (PQstatus(conexion) != CONNECTION_BAD) {
			R = PQexec(conexion, "BEGIN");
			if (PQresultStatus(R) != PGRES_COMMAND_OK)
				fprintf(stderr, "BEGIN command failed: %s", PQerrorMessage(conexion));
			PQclear(R);
			R = PQexec(conexion, consu);
			if (PQresultStatus(R) != PGRES_TUPLES_OK)
				fprintf(stderr, "insercion command failed: %s", PQerrorMessage(conexion));
			PQclear(R);
			R = PQexec(conexion, "END");
			if (PQresultStatus(R) != PGRES_COMMAND_OK)
				fprintf(stderr, "END command failed: %s", PQerrorMessage(conexion));
			PQclear(R);
			PQfinish(conexion);
		} else {
			printf("no conecto%s\n", PQerrorMessage(conexion));
		}
		if((MAX_MSG-1) > n) {
			return n;
		} else {
			flg1++;
		}
	} /* while */
}

void llenarLista(lista *vlis, char *serv, int vport){
	PGconn *conn;
	PGresult *res;
	char upd[500];
	int i;
	//upd =(char *) malloc (200 * sizeof(char));
	memset(upd, 0x0, 500);
	conn = PQsetdbLogin(serv,"5434",NULL,NULL,"bermann_gps_puerto","puerto","Puerto");
	if (PQstatus(conn) != CONNECTION_BAD) {
		sprintf(upd, "select av_serie, in_contenido, in_id from instruccion_in where in_estado = 0 and in_puerto = %d and in_id > %d order by in_id;", vport, vlis->max_inid);
		res = PQexec(conn, upd);
		if (res != NULL && PGRES_TUPLES_OK == PQresultStatus(res)) {
			for (i = 0; i < PQntuples(res); i++) {
				nuevoMsj(vlis, PQgetvalue(res,i,0), PQgetvalue(res,i,1), PQgetvalue(res,i,2));
			}
			PQclear(res);
		}
	}
	PQfinish(conn);
	//free (upd);
}

int nuevoMsj(lista *vlis, char *id, char *msj, char *vseq) {
	s_mensaje *nMsj;
	printf("para enviar : %s :: %s \n", id, msj);
	if ((nMsj = (s_mensaje *) calloc (1, sizeof (s_mensaje))) == NULL)
		return -1;
	if ((nMsj->av_serie = (char *) calloc ((strlen(id)+2), sizeof (char))) == NULL)
		return -1;
	if ((nMsj->mensaje = (char *) calloc ((strlen(msj)+2), sizeof (char))) == NULL)
		return -1;
	if ((nMsj->seq = (char *) calloc ((strlen(vseq)+2), sizeof (char))) == NULL)
		return -1;
	nMsj->sig = NULL;
	nMsj->ant = NULL;
	(void) time(&(nMsj->tiempo)) ;
	strcpy (nMsj->av_serie, id);
	strcpy (nMsj->mensaje, msj);
	strcpy (nMsj->seq, vseq);
	if (vlis->fin == NULL) {
		vlis->fin = nMsj;
		vlis->inicio = vlis->fin;
	} else {
		nMsj->ant = vlis->fin;
		vlis->fin->sig = nMsj;
		vlis->fin = nMsj;
	}
	return 1;
}

void MsjEnviado(lista *vlis, s_mensaje *leec, int k, char *serv) {
	//s_mensaje *leec;
	PGconn *conn;
	PGresult *res;
	char *upd;
	upd =(char *) calloc (200, sizeof(char));
	/*leec = vlis->inicio;
	vlis->inicio = leec->sig;
	if (vlis->inicio == NULL) {
		vlis->fin = NULL;
	}*/
	if (leec == vlis->inicio){
		vlis->inicio = leec->sig;
		if (vlis->inicio != NULL) {
			vlis->inicio->ant = NULL;
		}
	} 
	if (leec == vlis->fin){
		vlis->fin = leec->ant;
		if (vlis->fin != NULL) {
			vlis->fin->sig = NULL;
		}
	} 
	if (leec->ant != NULL && leec->sig != NULL) {
		leec->ant->sig = leec->sig;
		leec->sig->ant = leec->ant;
	}
	leec->ant = NULL;
	leec->sig = NULL;
	conn = PQsetdbLogin(serv,"5434",NULL,NULL,"bermann_gps_puerto","puerto","Puerto");
	if (PQstatus(conn) != CONNECTION_BAD) {
		sprintf(upd, "update instruccion_in set in_estado = 1, in_fecha_envio = now() where in_id = %s;", leec->seq);
		res = PQexec(conn, upd);
		if (PQresultStatus(res) != PGRES_COMMAND_OK)
			fprintf(stderr, "insercion command failed: %s", PQerrorMessage(conn));
		PQclear(res);
		PQfinish(conn);
	}
	free (upd);
	free (leec->av_serie);
	free (leec->mensaje);
	free (leec->seq);
	free (leec);
	
}


