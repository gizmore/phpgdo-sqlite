<?php
declare(strict_types=1);
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
use GDO\Util\FileUtil;
use GDO\Core\Application;
use GDO\Util\Strings;
use GDO\File\GDT_Files;

/**
 * SQLite DBMS module for phpgdo.
 *
 * @author gizmore
 * @version 7.0.3
 * @since 7.0.2
 */
final class Module_DBMS extends GDO_Module
{
	public int $priority = 7;

// 	private ?string $database = null; # absolute database file path
	private static ?\SQLite3 $sqLite = null; # connection
	private bool $inTransaction = false;

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

	##########
	### DB ###
	##########
	public function dbmsCreateDB(string $dbName): void
	{
// 		FileUtil::removeFile($this->dbPath($dbName));
	}

	public function dbmsUseDB(string $dbName): void
	{
		$this->dbmsOpen(GDO_DB_HOST, GDO_DB_USER, GDO_DB_PASS, $dbName, GDO_DB_PORT);
	}

	public function dbmsDropDB(string $dbName): void
	{
		$this->dbmsClose();
// 		if (@rename($this->dbPath($dbName), $this->dbPath($dbName.'.OLD')))
// 		{
			FileUtil::removeFile($this->dbPath($dbName), false);
// 		}
	}

	private function dbPath(string $filename): string
	{
		return GDO_PATH . 'protected/' . $filename;
	}

	################
	### DBMS API ###
	################
	/**
	 * Connect and setup a connection.
	 * If not installing, respect foreign keys and read-only.
	 */
	public function dbmsOpen(string $host, string $user, string $pass, string $database=null, int $port=3306): \SQLite3
	{
		$this->dbmsClose();
		self::$sqLite = new \SQLite3($this->dbPath($database));
		$this->dbmsQry('PRAGMA encoding = "UTF-8"');
		$this->dbmsQry('PRAGMA journal_mode = "'.GDO_DB_ENGINE.'"');
		if (!Application::$INSTANCE->isInstall())
		{
			$this->dbmsForeignKeys(true);
			$this->dbmsQry('PRAGMA query_only = "'.(GDO_DB_READONLY?'ON':'OFF').'"');
		}
		self::$sqLite->createCollation('ascii_cs', 'strnatcmp');
		self::$sqLite->createCollation('ascii_ci', 'strnatcasecmp');
		self::$sqLite->createCollation('utf8_cs', [$this, 'collate_utf8_cs']);
		self::$sqLite->createCollation('utf8_ci', [$this, 'collate_utf8_ci']);
		return self::$sqLite;
	}

	public function dbmsClose(): void
	{
		if (self::$sqLite)
		{
			self::$sqLite->close();
			self::$sqLite = null;
		}
	}

	public function dbmsForeignKeys(bool $foreignKeysEnabled): void
	{
		$onoff = $foreignKeysEnabled ? 'ON' : 'OFF';
		$this->dbmsQry("PRAGMA foreign_keys = {$onoff}");
	}

	public function dbmsQry(string $query): \SQLite3Result|bool
	{
		return $this->dbmsQuery($query, false);
	}

	public function dbmsQuery(string $query, bool $buffered=true): \SQLite3Result|bool
	{
//		try
//		{
			$result = @self::$sqLite->query($query);
			if (!$result)
			{
				throw new GDO_DBException('err_db', [$this->dbmsErrno(), $this->dbmsError(), html($query)]);
			}
			if  (str_starts_with($query, 'SELECT'))
			{
				return $result;
			}
			return true;
//		}
//		catch ()
//		return false;
	}

	public function dbmsFree(\SQLite3Result $result): void
	{
		$result->finalize();
	}

	public function dbmsFetchRow(\SQLite3Result $result): ?array
	{
		$row = $result->fetchArray(SQLITE3_NUM);
		return $row ? $this->_allToString($row) : null;
	}

	public function dbmsFetchAllRows(\SQLite3Result $result): array
	{
		return $this->_fetchAllB($result, SQLITE3_NUM);
	}

	public function dbmsFetchAssoc(\SQLite3Result $result): ?array
	{
		$row = $result->fetchArray(SQLITE3_ASSOC);
		return $row ? $this->_allToString($row) : null;
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
			$back[] = $this->_allToString($row);
		}
		return $back;
	}

	private function _allToString(array $row): array
	{
		foreach ($row as $key => $value)
		{
			$row[$key] = $value === null ? $value : (string) $value;
		}
		return $row;
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
		return self::$sqLite->lastInsertRowID();
	}

	public function dbmsAffected(): int
	{
		return self::$sqLite->changes();
	}

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
		if ($this->inTransaction)
		{
			$this->inTransaction = false;
			$this->dbmsQry("COMMIT");
		}
	}

	public function dbmsRollback(): void
	{
		if ($this->inTransaction)
		{
			$this->inTransaction = false;
			$this->dbmsQry("ROLLBACK");
		}
	}

	/**
	 * @TODO In case sqlite session locking is required, implement an flock based one.
	 */
	public function dbmsLock(string $lock, int $timeout=30): bool
	{
		return true;
	}

	public function dbmsUnlock(string $lock): bool
	{
		return true;
	}

	public function dbmsError(): string
	{
		return self::$sqLite->lastErrorMsg();
	}

	public function dbmsErrno(): int
	{
		return self::$sqLite->lastErrorCode();
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
				$primary[] = $column->name;
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
				$columns[] = "UNIQUE({$column->name})";
			}
		}

		foreach ($gdo->gdoColumnsCache() as $column)
		{
			if ( ($column instanceof GDT_Object) &&
				 (!$column instanceof GDT_Files) )
			{
				$columns[] = $this->_objfk($column);
			}
		}

		$columnsCode = implode(",\n", $columns);

		$query = "CREATE TABLE IF NOT EXISTS {$gdo->gdoTableName()} ".
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
			if ($gdt->getName() === 'image_files')
			{
				xdebug_break();
			}
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
	public function dbmsEscapeSearch(string $var): string
	{
		return str_replace(['\\', "'", chr(0)], ['\\\\', '\'\'', "'||char(0)||'"], $var);
	}

	public function dbmsEscape(string $var): string
	{
		return str_replace(['\\', "'", chr(0)], ['\\\\', '\'\'', "'||char(0)||'"], $var);
	}

	public function dbmsQuote(string $var): string
	{
		return sprintf("'%s'", $this->dbmsEscape($var));
	}

	public function dbmsRandom(): string
	{
		return 'RANDOM()';
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

	public function collate_utf8_cs(string $a, string $b): int
	{
		return Strings::compare($a, $b, true);
	}

	public function collate_utf8_ci(string $a, string $b): int
	{
		return Strings::compare($a, $b, false);
	}

	###############
	### Columns ###
	###############
	public function Core_GDT_AutoInc(GDT_AutoInc $gdt): string
	{
		return "{$gdt->name} INTEGER PRIMARY KEY NOT NULL";
	}

	public function Core_GDT_Int(GDT_Int $gdt): string
	{
		$unsigned = $gdt->unsigned ? ' UNSIGNED' : '';
		return "{$gdt->name} {$this->gdoSizeDefine($gdt)}INT{$unsigned}{$this->gdoNullDefine($gdt)}{$this->gdoInitialDefine($gdt)}";
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
		return "{$gdt->name} TINYINT UNSIGNED {$this->gdoNullDefine($gdt)}{$this->gdoInitialDefine($gdt)}";
	}

	public function Core_GDT_Float(GDT_Float $gdt): string
	{
		$unsigned = $gdt->unsigned ? " UNSIGNED" : GDT::EMPTY_STRING;
		return "{$gdt->name} FLOAT{$unsigned}{$this->gdoNullDefine($gdt)}{$this->gdoInitialDefine($gdt)}";
	}

	public function GDT_Decimal(GDT_Decimal $gdt): string
	{
		$digits = sprintf("%d,%d", $gdt->digitsBefore + $gdt->digitsAfter, $gdt->digitsAfter);
		return "{$gdt->name} DECIMAL($digits){$this->gdoNullDefine($gdt)}{$this->gdoInitialDefine($gdt)}";
	}

	public function Core_GDT_Char(GDT_Char $gdt): string
	{
		$char = $gdt->isBinary() ? 'BLOB' : 'CHAR';
		return "{$gdt->name} $char({$gdt->max})" .
			$this->gdoNullDefine($gdt) .
			$this->gdoInitialDefine($gdt);
	}

	public function Core_GDT_String(GDT_String $gdt): string
	{
		$char = $gdt->isBinary() ? 'BLOB' : 'VARCHAR';
		$null = $this->gdoNullDefine($gdt);
		return "{$gdt->name} {$char}({$gdt->max}) {$null}";
	}

	public function Core_GDT_Text(GDT_Text $gdt): string
	{
		return $gdt->name . ' ' . $this->Core_GDT_TextB($gdt);
	}

	public function Core_GDT_TextB(GDT_Text $gdt): string
	{
		$char = $gdt->isBinary() ? 'BLOB' : 'TEXT';
		return "{$char}({$gdt->max}) {$this->gdoNullDefine($gdt)}{$this->gdoInitialDefine($gdt)}";
	}

	public function Core_GDT_CreatedAt(GDT_CreatedAt $gdt) : string
	{
		$len = strlen('YYYY-MM-DD HH:ii:ss');
		$len += $gdt->millis ? 1 : 0;
		$len += $gdt->millis;
		return "{$gdt->name} CHAR({$len}) {$this->gdoNullDefine($gdt)}";
	}

	public function Date_GDT_Date(GDT_Date $gdt) : string
	{
		return "{$gdt->name} DATE {$this->gdoNullDefine($gdt)}{$this->gdoInitialDefine($gdt)}";
	}

	public function Date_GDT_DateTime(GDT_DateTime $gdt) : string
	{
		return "{$gdt->name} DATETIME({$gdt->millis}) {$this->gdoNullDefine($gdt)}{$this->gdoInitialDefine($gdt)}";
	}

	public function Date_GDT_Time(GDT_Time $gdt) : string
	{
		return "{$gdt->name} TIME {$this->gdoNullDefine($gdt)}{$this->gdoInitialDefine($gdt)}";
	}

	public function Date_GDT_Timestamp(GDT_Timestamp $gdt) : string
	{
		return "{$gdt->name} TIMESTAMP({$gdt->millis}){$this->gdoNullDefine($gdt)}{$this->gdoInitialDefine($gdt)}";
	}

	public function Core_GDT_ObjectSelect(GDT_ObjectSelect $gdt): string
	{
		return $this->Core_GDT_Object($gdt);
	}

	public function File_GDT_Files(GDT_Files $gdt): string
	{
		return '';
	}

	/**
	 * Take the foreign key primary key definition and use str_replace to convert to foreign key definition.
	 */
	public function Core_GDT_Object($gdt): string
	{
		if ($gdt instanceof GDT_Files)
		{
			return '';
		}

		if ( !($table = $gdt->table))
		{
			throw new GDO_Error('err_gdo_object_no_table', [
				$gdt->name,
			]);
		}
		$tableName = $table;
		if ( !($primaryKey = $table->gdoPrimaryKeyColumn()))
		{
			throw new GDO_Error('err_gdo_no_primary_key', [
				$tableName,
				$gdt->name,
			]);
		}
		$define = $primaryKey->gdoColumnDefine();
		$define = str_replace($primaryKey->name, $gdt->name, $define);
		$define = str_replace(' NOT NULL', '', $define);
		$define = str_replace(' PRIMARY KEY', '', $define);
		$define = str_replace(' AUTO_INCREMENT', '', $define);
		$define = preg_replace('#,FOREIGN KEY .* ON UPDATE (?:CASCADE|RESTRICT|SET NULL)#', '', $define);
		return "$define{$this->gdoNullDefine($gdt)}\n";
	}

	private function _objfk(GDT_Object $gdt): string
	{
		$table = $gdt->table;
		$tableName = $table->gdoTableName();
		$primaryKey = $table->gdoPrimaryKeyColumn();
		$on = $primaryKey->name;
		return "FOREIGN KEY ({$gdt->name}) REFERENCES $tableName($on)";
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
