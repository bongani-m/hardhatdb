class AccountsController < ApplicationController
  before_action :set_account, only: %i[show edit update destroy]

  def index
    @accounts = Account.includes(:notes).order(:name, :email)
  end

  def show
  end

  def new
    @account = Account.new(status: 1)
  end

  def create
    @account = Account.new(account_params)
    note = params.dig(:account, :note).to_s.strip
    if note.blank?
      @account.errors.add(:base, "Note is required")
      return render(:new, status: :unprocessable_entity)
    end

    Account.transaction do
      @account.save!
      @account.notes.create!(body: note)
    end
    redirect_to @account, notice: "Account created on #{Current.node}."
  rescue ActiveRecord::RecordInvalid
    render :new, status: :unprocessable_entity
  end

  def edit
  end

  def update
    if @account.update(account_params)
      redirect_to @account, notice: "Account updated."
    else
      render :edit, status: :unprocessable_entity
    end
  end

  def destroy
    Account.transaction do
      Note.where(account_id: @account.id).delete_all
      @account.delete
    end
    redirect_to accounts_path, notice: "Account deleted."
  end

  private

  def set_account
    @account = Account.find(params[:id])
  end

  def account_params
    permitted = params.require(:account).permit(:id, :name, :email, :status, :tags_text, :note)
    permitted.delete(:id) if permitted[:id].blank?
    permitted.except(:note)
  end
end
