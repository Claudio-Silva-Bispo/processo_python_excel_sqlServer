def inserir(self, pacote: List[str]) -> List[List[Any]]:
        logs = []
        try:
            conexao = self.conectar('nome-banco')
            cursor = conexao.cursor()
            for i, insert in enumerate(pacote):
                try:
                    cursor.execute(insert)
                    logs.append(['SUCESSO', "OK", "OK"])
                except Exception as e:
                    logs.append(['ERRO', e, insert])
                
                if (i + 1) % 10000 == 0:
                    cursor.commit()
            
            cursor.commit()
        except Exception as e:
            print(f"Erro ao inserir dados: {e}")
        finally:
            cursor.close()
            conexao.close()
        
        return logs
