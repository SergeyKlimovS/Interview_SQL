-- Для того, чтобы повторное выполнение скрипта не приводило к возникновению ошибки, необходимо проверять существование в БД создаваемого объекта. (Create or alter) 1/17
create procedure syn.usp_ImportFileCustomerSeasonal
	@ID_Record int
-- AS заглавными буквами. 2/17
as
set nocount on
begin
	/* 
		Для объявления переменных declare используется один раз. 
		Дополнительное объявление переменных через declare используется только.
		Если необходимо использовать ранее объявленную переменную для определения значения объявляемой. 3/17
	*/
	declare 
		@RowCount int = (select count(*) from syn.SA_CustomerSeasonal)
		-- Рекомендуется при объявлении типов не использовать длину поля max. 4/17
		,@ErrorMessage varchar(max)
	-- У комментария тот же уровень отступа, что и у строки/блока, к которому он относится. 5/17
	-- Комментарии пишутся грамотно, с соблюдением правил русского языка и точек в конце предложений. 6/17
-- Проверка на корректность загрузки
	if not exists (
		-- Содержимое exists оформляется с одним отступом. 7/17
		select 1
		-- Неверный алиас, должно быть imf. 8/17
		from syn.ImportFile as f
		where f.ID = @ID_Record
			and f.FlagLoaded = cast(1 as bit)
	)
	-- Лишний TAB. 9/17
	begin
		set @ErrorMessage = 'Ошибка при загрузке файла, проверьте корректность данных'

		raiserror(@ErrorMessage, 3, 1)
		return
	end
	-- Отсутствие пробела. 10/17
	--Чтение из слоя временных данных
	select
		c.ID as ID_dbo_Customer
		,cst.ID as ID_CustomerSystemType
		,s.ID as ID_Season
		,cast(cs.DateBegin as date) as DateBegin
		,cast(cs.DateEnd as date) as DateEnd
		,c_dist.ID as ID_dbo_CustomerDistributor 
		,cast(isnull(cs.FlagActive, 0) as bit) as FlagActive
	into #CustomerSeasonal
	from syn.SA_CustomerSeasonal cs
		-- Все виды join указываются явно, Inner join. 11/17
		join dbo.Customer as c on c.UID_DS = cs.UID_DS_Customer
			and c.ID_mapping_DataSource = 1
		join dbo.Season as s on s.Name = cs.Season
		join dbo.Customer as c_dist on c_dist.UID_DS = cs.UID_DS_CustomerDistributor
			-- Не существует алиаса cd, dbo.Customer as c. 12/17
			and cd.ID_mapping_DataSource = 1
		-- При соединении таблиц сперва после on указываем поле присоединяемой таблицы. 13/17
		join syn.CustomerSystemType as cst on cs.CustomerSystemType = cst.Name
	where try_cast(cs.DateBegin as date) is not null
		and try_cast(cs.DateEnd as date) is not null
		and try_cast(isnull(cs.FlagActive, 0) as bit) is not null

	-- Определяем некорректные записи.
	-- Добавляем причину, по которой запись считается некорректной.
	select
		cs.*
		,case
			when c.ID is null then 'UID клиента отсутствует в справочнике "Клиент"'
			when c_dist.ID is null then 'UID дистрибьютора отсутствует в справочнике "Клиент"'
			when s.ID is null then 'Сезон отсутствует в справочнике "Сезон"'
			when cst.ID is null then 'Тип клиента отсутствует в справочнике "Тип клиента"'
			when try_cast(cs.DateBegin as date) is null then 'Невозможно определить Дату начала'
			when try_cast(cs.DateEnd as date) is null then 'Невозможно определить Дату окончания'
			when try_cast(isnull(cs.FlagActive, 0) as bit) is null then 'Невозможно определить Активность'
		end as Reason
	into #BadInsertedRows
	from syn.SA_CustomerSeasonal as cs
	left join dbo.Customer as c on c.UID_DS = cs.UID_DS_Customer
		and c.ID_mapping_DataSource = 1
	left join dbo.Customer as c_dist on c_dist.UID_DS = cs.UID_DS_CustomerDistributor and c_dist.ID_mapping_DataSource = 1
	left join dbo.Season as s on s.Name = cs.Season
	left join syn.CustomerSystemType as cst on cst.Name = cs.CustomerSystemType
	where cc.ID is null
		or cd.ID is null
		or s.ID is null
		or cst.ID is null
		or try_cast(cs.DateBegin as date) is null
		or try_cast(cs.DateEnd as date) is null
		or try_cast(isnull(cs.FlagActive, 0) as bit) is null

	-- Обработка данных из файла
	-- Перед название таблицы, в которую осуществляется merge, into не указывается. 14/17
	-- Стандартные алиасы: Использовать t для целевой таблицы и s для источника данных, t.CustomerSeasonal. 15/17
	merge into syn.CustomerSeasonal as st
	using (
		select
			cs_temp.ID_dbo_Customer
			,cs_temp.ID_CustomerSystemType
			,cs_temp.ID_Season
			,cs_temp.DateBegin
			,cs_temp.DateEnd
			,cs_temp.ID_dbo_CustomerDistributor
			,cs_temp.FlagActive
		from #CustomerSeasonal as cs_temp
	) as s on s.ID_dbo_Customer = cs.ID_dbo_Customer
		and s.ID_Season = cs.ID_Season
		and s.DateBegin = cs.DateBegin
	-- then записывается на одной строке с when, независимо от наличия дополнительных условий. 16/17
	when matched 
		and t.ID_CustomerSystemType <> s.ID_CustomerSystemType then
		update
		set
			ID_CustomerSystemType = s.ID_CustomerSystemType
			,DateEnd = s.DateEnd
			,ID_dbo_CustomerDistributor = s.ID_dbo_CustomerDistributor
			,FlagActive = s.FlagActive
	when not matched then
		insert (ID_dbo_Customer, ID_CustomerSystemType, ID_Season, DateBegin, DateEnd, ID_dbo_CustomerDistributor, FlagActive)
		values (s.ID_dbo_Customer, s.ID_CustomerSystemType, s.ID_Season, s.DateBegin, s.DateEnd, s.ID_dbo_CustomerDistributor, s.FlagActive)
	-- ; Ставится в конце последней строки конструкции merge. 17/17
	;

	-- Информационное сообщение
	begin
		select @ErrorMessage = concat('Обработано строк: ', @RowCount)

		raiserror(@ErrorMessage, 1, 1)

		-- Формирование таблицы для отчетности
		select top 100
			Season as 'Сезон'
			,UID_DS_Customer as 'UID Клиента'
			,Customer as 'Клиент'
			,CustomerSystemType as 'Тип клиента'
			,UID_DS_CustomerDistributor as 'UID Дистрибьютора'
			,CustomerDistributor as 'Дистрибьютор'
			,isnull(format(try_cast(DateBegin as date), 'dd.MM.yyyy', 'ru-RU'), DateBegin) as 'Дата начала'
			,isnull(format(try_cast(DateEnd as date), 'dd.MM.yyyy', 'ru-RU'), DateEnd) as 'Дата окончания'
			,FlagActive as 'Активность'
			,Reason as 'Причина'
		from #BadInsertedRows

		return
	end
-- Одной пустой строкой до и после отделяются разные логические блоки кода. 18/17
end
