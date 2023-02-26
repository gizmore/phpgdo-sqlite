<?php
namespace GDO\DBMS;

use GDO\Core\GDO_Module;
use GDO\Core\GDO;
use GDO\DB\Database;
use GDO\Core\GDT;
use GDO\Core\GDT_Int;
use GDO\Core\GDT_Float;
use GDO\Core\GDT_String;
use GDO\Core\GDO_Error;
use GDO\Core\GDT_DBField;
use GDO\Date\GDT_Date;
use GDO\Date\GDT_Time;
use GDO\Date\GDT_Timestamp;
use GDO\Core\GDT_CreatedAt;
use GDO\Core\GDT_Checkbox;
use GDO\Core\GDT_Text;
use GDO\Core\GDT_Object;
use GDO\Core\GDT_AutoInc;
use GDO\Core\GDT_Char;
use GDO\Core\GDT_Decimal;
use GDO\Core\GDT_Enum;
use GDO\Core\GDT_Index;
use GDO\Date\GDT_DateTime;
use GDO\Core\GDO_DBException;
use GDO\Core\GDT_ObjectSelect;

/**
 * SQLite DBMS module for phpgdo.
 * 
 * @author gizmore
 * @version 7.0.2
 * @since 7.0.2
 */
final class Module_DBMS extends GDO_Module
{
	public int $priority = 7;
	
	private ?\SQLite3 $sqLite = null;

	##############
	### Module ###
	##############
	public function checkSystemDependencies() : bool
	{
		if (!class_exists('\SQLite3', false))
		{
			return $this->errorSystemDependency('err_php_extension', ['sqlite']);
		}
		return true;
	}
	
	public function onModuleInit(): void
	{
		ini_set('sqlite3.defensive', true);
	}
	
	################
	### DBMS API ###
	################
	public function dbmsOpen(string $host, string $user, string $pass, string $database=null, int $port=3306): \SQLite3
	{
		$path = GDO_PATH . GDO_FILES_DIR . '/' . $database;
		$this->sqLite = new \SQLite3($path);
		$this->dbmsQry('PRAGMA encoding = "UTF-8"');
		$this->dbmsQry('PRAGMA encoding = "UTF-8"');
		$this->dbmsForeignKeys(true);
		return $this->sqLite;
	}
	public function dbmsClose(): void
	{
		$this->sqLite->close();
		unset($this->sqLite);
	}
	
	public function dbmsForeignKeys(bool $foreignKeysEnabled): void
	{
		$onoff = $foreignKeysEnabled ? 'ON' : 'OFF';
		$this->dbmsQry("PRAGMA foreign_keys = {$onoff}");
	}
	
	public function dbmsQry(string $query)
	{
		return $this->dbmsQuery($query, false);
	}
	
	public function dbmsQuery(string $query, bool $buffered=true)
	{
		$result = @$this->sqLite->query($query);
		if (!$result)
		{
			throw new GDO_DBException('err_db', [$this->dbmsErrno(), $this->dbmsError(), html($query)]);
		}
		return $result;
	}
	
	public function dbmsFree(\SQLite3Result $result): void
	{
		$result->finalize();
	}
	
	public function dbmsFetchRow(\SQLite3Result $result): ?array
	{
		$row = $result->fetchArray(SQLITE3_NUM);
		return $row ? $row : null;
	}
	
	public function dbmsFetchAllRows(\SQLite3Result $result): array
	{
		return $this->_fetchAllB($result, SQLITE3_NUM);
	}
	
	public function dbmsFetchAssoc(\SQLite3Result $result): ?array
	{
		$row = $result->fetchArray(SQLITE3_ASSOC);
		return $row ? $row : null;
	}
	
	public function dbmsFetchAllAssoc(\SQLite3Result $result): array
	{
		return $this->_fetchAllB($result, SQLITE3_ASSOC);
	}
	
	private function _fetchAllB(\SQLite3Result $result, int $mode): array
	{
		$back = [];
		while ($row = $result->fetchArray($mode))
		{
			$back[] = $row;
		}
		return $back;
	}
	
	/**
	 * @deprecated slow
	 */
	public function dbmsNumRows(\SQLite3Result $result): int
	{
		$count = 0;
		while ($result->fetchArray(SQLITE3_NUM))
		{
			$count++;
		}
		$result->reset();
		return $count;
	}
	
	public function dbmsInsertId(): int
	{
		return $this->sqLite->lastInsertRowID();
	}
	
	public function dbmsAffected(): int
	{
		return $this->sqLite->changes();
	}
	
	private bool $inTransaction = false;
	
	public function dbmsBegin(): void
	{
		if (!$this->inTransaction)
		{
			$this->inTransaction = true;
			$this->dbmsQry("BEGIN");
		}
	}
	
	public function dbmsCommit(): void
	{
		$this->inTransaction = false;
		$this->dbmsQry("COMMIT");
	}
	
	public function dbmsRollback(): void
	{
		$this->inTransaction = false;
		$this->dbmsQry("ROLLBACK");
	}
	
	/**
	 * @TODO In case sqlite session locking is required, implement an flock based one.
	 */
	public function dbmsLock(string $lock, int $timeout=30): void
	{
	}
	
	public function dbmsUnlock(string $lock): void
	{
	}
	
	public function dbmsError(): string
	{
		return $this->sqLite->lastErrorMsg();
	}
	
	public function dbmsErrno(): int
	{
		return $this->sqLite->lastErrorCode();
	}
	
	############
	### Bulk ###
	############
	public function dbmsExecFile(string $path): void
	{
		$fh = fopen($path, 'r');
		$command = '';
		while ($line = fgets($fh))
		{
			$line = trim($line);
			
			if ( (str_starts_with($line, '-- ')) ||
				(str_starts_with($line, '/*')) )
			{
				# skip comments
				continue;
			}
			
			# Append to command
			$command .= $line;
			
			# Finished command
			if (str_ends_with($line, ';'))
			{
				# Most likely a write
				$this->dbmsQry($command);
				$command = '';
			}
		}
	}
	
	##########
	### DB ###
	##########
	public function dbmsCreateDB(string $dbName): void
	{
		$this->dbmsQry("CREATE DATABASE {$dbName}");
	}
	
	public function dbmsUseDB(string $dbName): void
	{
		$this->dbmsQry("USE {$dbName}");
	}
	
	public function dbmsDropDB(string $dbName): void
	{
		$this->dbmsQry("DROP DATABASE {$dbName}");
	}
	
	##############
	### Schema ###
	##############
	public function dbmsTableExists(string $tableName): bool
	{
		$dbName = Database::instance()->usedb;
		$query = "SELECT EXISTS (SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_SCHEMA LIKE '{$dbName}' AND TABLE_TYPE LIKE 'BASE TABLE' AND TABLE_NAME = '{$tableName}');";
		return !!$this->dbmsQry($query);
	}
	
	public function dbmsCreateTable(GDO $gdo): void
	{
		$query = $this->dbmsCreateTableCode($gdo);
		$this->dbmsQry($query);
	}
	
	public function dbmsCreateTableCode(GDO $gdo): string
	{
		$columns = [];
		$primary = [];
		
		foreach ($gdo->gdoColumnsCache() as $column)
		{
			if ($define = $column->gdoColumnDefine())
			{
				$columns[] = $define;
			}
			if (isset($column->primary) && $column->primary) # isPrimary() not used, because of AutoInc hack.
			{
				$primary[] = $column->identifier();
			}
		}
		
		if (count($primary))
		{
			$primary = implode(',', $primary);
			$columns[] = "PRIMARY KEY ($primary) "; # . Database::PRIMARY_USING;
		}
		
		foreach ($gdo->gdoColumnsCache() as $column)
		{
			if ($column->isUnique())
			{
				$columns[] = "UNIQUE({$column->identifier()})";
			}
		}
		
		foreach ($gdo->gdoColumnsCache() as $column)
		{
			if ($column instanceof GDT_Object)
			{
				$columns[] = $this->_objfk($column);
			}
		}
		
		$columnsCode = implode(",\n", $columns);
		
		$query = "CREATE TABLE IF NOT EXISTS {$gdo->gdoTableIdentifier()} ".
			"(\n$columnsCode\n)\n";
		
		return $query;
	}
	
	public function dbmsTruncateTable(string $tableName): void
	{
		$this->dbmsQry("TRUNCATE TABLE {$tableName}");
	}
	
	public function dbmsDropTable(string $tableName): void
	{
		$this->dbmsQry("DROP TABLE IF EXISTS {$tableName}");
	}
	
	public function dbmsSchema(GDT $gdt): string
	{
		$classes = class_parents($gdt);
		array_unshift($classes, get_class($gdt));
		foreach ($classes as $classname)
		{
			$classname = substr($classname, 4);
			$classname = str_replace('\\', '_', $classname);
			if (method_exists($this, $classname))
			{
				return call_user_func([$this, $classname], $gdt);
			}
		}
		throw new GDO_Error('err_gdt_column_define_missing', [$gdt->getName(), get_class($gdt)]);
	}
	
	##############
	### Compat ###
	##############
	public function dbmsEscape(string $var): string
	{
		return str_replace(['\\', "'"], ['\\\\', '\\\''], $var);
	}
	
	public function dbmsQuote(string $var): string
	{
		return sprintf("'%s'", $this->dbmsEscape($var));
	}
	
	public function dbmsConcat(string ...$fields): string
	{
		return implode(' || ', $fields);
	}
	
	public function dbmsTimestamp(string $arg): string
	{
		return sprintf("strftime('%%s', %s)", $arg);
	}
	
	public function dbmsFromUnixtime(int $time=0): string
	{
		$time = $time?:time();
		return "DATETIME({$time}, 'unixepoch')";
	}
	
	###############
	### Columns ###
	###############
	public function Core_GDT_AutoInc(GDT_AutoInc $gdt): string
	{
		return "{$gdt->identifier()} INTEGER PRIMARY KEY NOT NULL";
	}
	
	public function Core_GDT_Int(GDT_Int $gdt): string
	{
		$unsigned = $gdt->unsigned ? ' UNSIGNED' : '';
		return "{$gdt->identifier()} {$this->gdoSizeDefine($gdt)}INT{$unsigned}{$this->gdoNullDefine($gdt)}{$this->gdoInitialDefine($gdt)}";
	}
	
	/**
	 * SQLite does not feature enums. :(
	 * We use a string instead, until a better solution is found.
	 */
	public function Core_GDT_Enum(GDT_Enum $gdt): string
	{
		$max = 0;
		foreach ($gdt->enumValues as $val)
		{
			$len = mb_strlen($val);
			$max = $len > $max ? $len : $max;
		}
		$str = GDT_String::make($gdt->name)->max($max)->notNull($gdt->notNull)->primary($gdt->primary);
		return $this->Core_GDT_String($str);
	}
	
	public function Core_GDT_Checkbox(GDT_Checkbox $gdt) : string
	{
		return "{$gdt->identifier()} TINYINT UNSIGNED {$this->gdoNullDefine($gdt)}{$this->gdoInitialDefine($gdt)}";
	}
	
	public function Core_GDT_Float(GDT_Float $gdt): string
	{
		$unsigned = $this->unsigned ? " UNSIGNED" : GDT::EMPTY_STRING;
		return "{$gdt->identifier()} FLOAT{$unsigned}{$this->gdoNullDefine($gdt)}{$this->gdoInitialDefine($gdt)}";
	}
	
	public function GDT_Decimal(GDT_Decimal $gdt): string
	{
		$digits = sprintf("%d,%d", $gdt->digitsBefore + $gdt->digitsAfter, $gdt->digitsAfter);
		return "{$gdt->identifier()} DECIMAL($digits){$this->gdoNullDefine($gdt)}{$this->gdoInitialDefine($gdt)}";
	}

	public function Core_GDT_Char(GDT_Char $gdt): string
	{
		return "{$gdt->identifier()} CHAR({$gdt->max})" .
			$this->gdoNullDefine($gdt) .
			$this->gdoInitialDefine($gdt);
	}
	
	public function Core_GDT_String(GDT_String $gdt): string
	{
		$null = $this->gdoNullDefine($gdt);
		return "{$gdt->identifier()} VARCHAR({$gdt->max}) {$null}";
	}
	
	public function Core_GDT_Text(GDT_Text $gdt): string
	{
		return $gdt->identifier() . ' ' . $this->Core_GDT_TextB($gdt);
	}
	
	public function Core_GDT_TextB(GDT_Text $gdt): string
	{
		return "TEXT({$gdt->max}) {$this->gdoNullDefine($gdt)}{$this->gdoInitialDefine($gdt)}";
	}
	
	public function Core_GDT_CreatedAt(GDT_CreatedAt $gdt) : string
	{
		$len = strlen('YYYY-MM-DD HH:ii:ss');
		$len += $gdt->millis ? 1 : 0;
		$len += $gdt->millis;
		return "{$gdt->identifier()} CHAR({$len}) {$this->gdoNullDefine($gdt)}";
	}
	
	public function Date_GDT_Date(GDT_Date $gdt) : string
	{
		return "{$gdt->identifier()} DATE {$this->gdoNullDefine($gdt)}{$this->gdoInitialDefine($gdt)}";
	}
	
	public function Date_GDT_DateTime(GDT_DateTime $gdt) : string
	{
		return "{$gdt->identifier()} DATETIME({$gdt->millis}) {$this->gdoNullDefine($gdt)}{$this->gdoInitialDefine($gdt)}";
	}
	
	public function Date_GDT_Time(GDT_Time $gdt) : string
	{
		return "{$gdt->identifier()} TIME {$this->gdoNullDefine($gdt)}{$this->gdoInitialDefine($gdt)}";
	}
	
	public function Date_GDT_Timestamp(GDT_Timestamp $gdt) : string
	{
		return "{$gdt->identifier()} TIMESTAMP({$gdt->millis}){$this->gdoNullDefine($gdt)}{$this->gdoInitialDefine($gdt)}";
	}
	
	public function Core_GDT_ObjectSelect(GDT_ObjectSelect $gdt): string
	{
		return $this->Core_GDT_Object($gdt);
	}
	
	/**
	 * Take the foreign key primary key definition and use str_replace to convert to foreign key definition.
	 */
	public function Core_GDT_Object($gdt): string
	{
		if ( !($table = $gdt->table))
		{
			throw new GDO_Error('err_gdo_object_no_table', [
				$gdt->identifier(),
			]);
		}
		$tableName = $table->gdoTableIdentifier();
		if ( !($primaryKey = $table->gdoPrimaryKeyColumn()))
		{
			throw new GDO_Error('err_gdo_no_primary_key', [
				$tableName,
				$gdt->identifier(),
			]);
		}
		$define = $primaryKey->gdoColumnDefine();
		$define = str_replace($primaryKey->identifier(), $gdt->identifier(), $define);
		$define = str_replace(' NOT NULL', '', $define);
		$define = str_replace(' PRIMARY KEY', '', $define);
		$define = str_replace(' AUTO_INCREMENT', '', $define);
		$define = preg_replace('#,FOREIGN KEY .* ON UPDATE (?:CASCADE|RESTRICT|SET NULL)#', '', $define);
		return "$define{$this->gdoNullDefine($gdt)}\n";
	}
	
	private function _objfk(GDT_Object $gdt): string
	{
		$table = $gdt->table;
		$tableName = $table->gdoTableIdentifier();
		$primaryKey = $table->gdoPrimaryKeyColumn();
		$on = $primaryKey->identifier();
		return "FOREIGN KEY ({$gdt->identifier()}) REFERENCES $tableName($on)";
	}
	
	public function Core_GDT_Index(GDT_Index $gdt)
	{
		return "{$this->gdoFulltextDefine($gdt)} INDEX({$gdt->indexColumns}) {$this->gdoUsingDefine($gdt)}";
	}
	
	##############
	### Helper ###
	##############
	private function gdoNullDefine(GDT_DBField $gdt) : string
	{
		return $gdt->notNull ? ' NOT NULL' : ' NULL';
	}
	
	private function gdoInitialDefine(GDT_DBField $gdt) : string
	{
		return isset($gdt->initial) ? (' DEFAULT '.GDO::quoteS($gdt->initial)) : '';
	}
	
	private function gdoSizeDefine(GDT_Int $gdt): string
	{
		switch ($gdt->bytes)
		{
			case 1: return 'TINY';
			case 2: return 'MEDIUM';
			case 4: return '';
			case 8: return 'BIG';
			default: throw new GDO_Error('err_int_bytes_length', [$gdt->bytes]);
		}
	}
	
	private function gdoCharsetDefine(GDT_String $gdt) : string
	{
		return '';
	}
	
	private function gdoCollateDefine(GDT_String $gdt, bool $caseSensitive) : string
	{
		if (!$gdt->isBinary())
		{
			$append = $caseSensitive ? '_bin' : '_general_ci';
			return ' COLLATE ' . $this->gdoCharsetDefine($gdt) . $append;
		}
		return GDT::EMPTY_STRING;
	}
	
	private function gdoFulltextDefine(GDT_Index $gdt): string
	{
		return isset($gdt->indexFulltext) ? $gdt->indexFulltext : GDT::EMPTY_STRING;
	}
	
	private function gdoUsingDefine(GDT_Index $gdt)
	{
		return $gdt->indexUsing === false ? GDT::EMPTY_STRING : $gdt->indexUsing;
	}
	
	#################
	### Migration ###
	#################
	/**
	 * Automigrations are pretty kewl.
	 */
	public function dbmsAutoMigrate(GDO $gdo): void
	{
		# Remove old temp table
		$tablename = $gdo->gdoTableName();
		$temptable = "zzz_temp_{$tablename}";
		$this->dbmsDropTable($temptable);
		
		# create temp and copy as old
		$this->dbmsForeignKeys(false);
		# Do not! drop the temp table. It might contain live data from a failed upgrade
		$query = "SHOW CREATE TABLE $tablename";
		$result = $this->dbmsQry($query);
		$query = mysqli_fetch_row($result)[1];
		$query = str_replace($tablename, $temptable, $query);
		$this->dbmsQry($query);
		$query = "INSERT INTO $temptable SELECT * FROM $tablename";
		$this->dbmsQry($query);
		
		# drop existing and recreate as new
		$query = "DROP TABLE $tablename";
		$this->dbmsQry($query);
		$gdo->createTable(); # CREATE TABLE IF NOT EXIST
		
		# calculate columns and copy back in new
		if ($columns = $this->columnNames($gdo, $temptable))
		{
			$columns = implode(',', $columns);
			$query = "INSERT INTO $tablename ($columns) SELECT $columns FROM $temptable";
			$this->dbmsQry($query);
			
			# drop temp after all succeded.
			$query = "DROP TABLE $temptable";
			$this->dbmsQry($query);
		}
	}
	
	private function dbmsColumnNames(GDO $gdo, string $temptable): ?array
	{
		$db = GDO_DB_NAME;
		
		$query = "SELECT group_concat(COLUMN_NAME) FROM INFORMATION_SCHEMA.COLUMNS " .
			"WHERE TABLE_SCHEMA = '{$db}' AND TABLE_NAME = '{$temptable}'";
		$result = $this->dbmsQuery($query);
		$old = mysqli_fetch_array($result)[0];
		$old = explode(',', $old);
		
		$query = "SELECT group_concat(COLUMN_NAME) FROM INFORMATION_SCHEMA.COLUMNS " .
			"WHERE TABLE_SCHEMA = '{$db}' AND TABLE_NAME = '{$gdo->gdoTableName()}'";
		$result = $this->dbmsQuery($query);
		$new = mysqli_fetch_array($result)[0];
		$new = explode(',', $new);
		return ($old && $new) ?
			array_intersect($old, $new) : [];
	}
	
}
